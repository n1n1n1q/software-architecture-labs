import asyncio
import datetime
import json
import os
import random
import socket
import time
import uuid
from contextlib import asynccontextmanager

import grpc
import httpx
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from service_registry import ConfigServerClient

service_name = os.getenv("SERVICE_NAME", "facade-service")
instance_id = os.getenv("INSTANCE_ID", socket.gethostname())
http_port = int(os.getenv("HTTP_PORT", "8002"))
self_address = os.getenv("SELF_ADDRESS", f"http://{instance_id}:{http_port}")

logging_service_name = os.getenv("LOGGING_SERVICE_NAME", "logging-service")
counter_service_name = os.getenv("COUNTER_SERVICE_NAME", "counter-service")

logging_time_total = 0.0
counter_time_total = 0.0
metrics_lock = asyncio.Lock()

http_client: httpx.AsyncClient | None = None
config_client: ConfigServerClient | None = None
kafka_producer: AIOKafkaProducer | None = None
balance_updates_topic: str = "balance-updates"

logging_grpc_channels: dict[str, grpc.aio.Channel] = {}
logging_grpc_lock = asyncio.Lock()


def json_dumps(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def json_loads(raw: bytes) -> dict:
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


async def get_logging_channel(target: str) -> grpc.aio.Channel:
    async with logging_grpc_lock:
        channel = logging_grpc_channels.get(target)
        if channel is None:
            channel = grpc.aio.insecure_channel(
                target,
                options=[
                    ("grpc.max_send_message_length", 32 * 1024 * 1024),
                    ("grpc.max_receive_message_length", 32 * 1024 * 1024),
                ],
            )
            logging_grpc_channels[target] = channel
        return channel


async def call_logging(
    target: str, method: str, payload: dict, timeout: float = 10.0
) -> dict:
    channel = await get_logging_channel(target)
    callable_method = channel.unary_unary(
        f"/logging.LoggingService/{method}",
        request_serializer=json_dumps,
        response_deserializer=json_loads,
    )
    return await callable_method(payload, timeout=timeout)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, config_client, kafka_producer, balance_updates_topic

    print(f"[{instance_id}] Starting facade-service ...", flush=True)
    http_client = httpx.AsyncClient(timeout=10.0)

    config_client = ConfigServerClient()
    await config_client.register(service_name, instance_id, self_address)

    bootstrap_servers = await config_client.get_config_with_retry(
        "kafka.bootstrap.servers"
    )
    balance_updates_topic = await config_client.get_config_with_retry(
        "kafka.balance_updates.topic"
    )
    print(
        f"[{instance_id}] Kafka config: bootstrap={bootstrap_servers} topic={balance_updates_topic}",
        flush=True,
    )

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode(
            "utf-8"
        ),
        key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
        acks="all",
        enable_idempotence=True,
    )
    last_error: BaseException | None = None
    for attempt in range(1, 61):
        try:
            await kafka_producer.start()
            print(
                f"[{instance_id}] Kafka producer started (attempt {attempt})",
                flush=True,
            )
            break
        except (KafkaConnectionError, KafkaError, OSError) as exc:
            last_error = exc
            print(
                f"[{instance_id}] Kafka producer start attempt {attempt} failed: {exc}. Retrying in 2s...",
                flush=True,
            )
            await asyncio.sleep(2.0)
    else:
        raise RuntimeError(
            f"Could not connect to Kafka after 60 attempts: {last_error}"
        )

    yield

    print(f"[{instance_id}] Shutting down ...", flush=True)
    if kafka_producer is not None:
        await kafka_producer.stop()

    for channel in logging_grpc_channels.values():
        await channel.close()

    if config_client is not None:
        await config_client.unregister(service_name, instance_id)
        await config_client.close()

    if http_client is not None:
        await http_client.aclose()

    print(f"[{instance_id}] Shutdown complete", flush=True)


app = FastAPI(lifespan=lifespan)


class TransactionRequest(BaseModel):
    user_id: str
    amount: float


def normalize_grpc_target(address: str) -> str:
    """Logging instances register their gRPC endpoint as host:port (no scheme)."""
    if "://" in address:
        return address.split("://", 1)[1]
    return address


def normalize_http_url(address: str) -> str:
    if address.startswith("http://") or address.startswith("https://"):
        return address.rstrip("/")
    return f"http://{address}".rstrip("/")


async def discover_logging_targets() -> list[str]:
    assert config_client is not None
    addresses = await config_client.discover(logging_service_name)
    return [normalize_grpc_target(address) for address in addresses]


async def discover_counter_urls() -> list[str]:
    assert config_client is not None
    addresses = await config_client.discover(counter_service_name)
    return [normalize_http_url(address) for address in addresses]


async def logging_post_with_failover(payload: dict):
    targets = await discover_logging_targets()
    if not targets:
        raise HTTPException(
            status_code=503,
            detail="No logging-service instances registered in config-server",
        )

    shuffled = random.sample(targets, k=len(targets))
    last_error: BaseException | None = None
    for attempt, target in enumerate(shuffled, start=1):
        try:
            print(
                f"[{instance_id}] Logging attempt {attempt}/{len(shuffled)} -> {target}",
                flush=True,
            )
            started = time.perf_counter()
            response = await call_logging(
                target, "LogTransaction", payload, timeout=10.0
            )
            elapsed = time.perf_counter() - started
            if response.get("status") == "ok":
                print(f"[{instance_id}] Logged via {target}: {response}", flush=True)
                return response, elapsed
            raise RuntimeError(response.get("message", "Unknown logging error"))
        except (grpc.RpcError, RuntimeError) as exc:
            print(f"[{instance_id}] Logging error on {target}: {exc}", flush=True)
            last_error = exc

    raise HTTPException(
        status_code=503,
        detail=f"All logging-service instances unavailable: {last_error}",
    )


async def logging_get_with_failover():
    targets = await discover_logging_targets()
    if not targets:
        raise HTTPException(
            status_code=503,
            detail="No logging-service instances registered in config-server",
        )

    shuffled = random.sample(targets, k=len(targets))
    last_error: BaseException | None = None
    for target in shuffled:
        try:
            started = time.perf_counter()
            response = await call_logging(target, "GetLogs", {}, timeout=10.0)
            elapsed = time.perf_counter() - started
            return response, elapsed
        except grpc.RpcError as exc:
            last_error = exc
            print(f"[{instance_id}] GetLogs error on {target}: {exc}", flush=True)

    raise HTTPException(
        status_code=503,
        detail=f"All logging-service instances unavailable: {last_error}",
    )


async def counter_get_with_failover(path: str):
    """Try every registered counter-service. If none works, return None instead of raising."""
    assert http_client is not None
    urls = await discover_counter_urls()
    if not urls:
        print(f"[{instance_id}] No counter-service instances registered", flush=True)
        return None, 0.0

    shuffled = random.sample(urls, k=len(urls))
    last_error: BaseException | None = None
    for base_url in shuffled:
        url = f"{base_url}{path}"
        try:
            started = time.perf_counter()
            response = await http_client.get(url, timeout=3.0)
            elapsed = time.perf_counter() - started
            response.raise_for_status()
            return response.json(), elapsed
        except (httpx.HTTPError, OSError) as exc:
            print(
                f"[{instance_id}] counter-service GET {url} failed: {exc}", flush=True
            )
            last_error = exc

    print(
        f"[{instance_id}] All counter-service instances unavailable: {last_error}",
        flush=True,
    )
    return None, 0.0


@app.post("/transaction")
async def process_transaction(request: TransactionRequest):
    if kafka_producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer is not ready")

    timestamp = datetime.datetime.now()
    transaction_id = uuid.uuid4()
    payload = {
        "transaction_id": str(transaction_id),
        "user_id": request.user_id,
        "amount": request.amount,
        "timestamp": timestamp.isoformat(),
    }

    log_task = asyncio.create_task(logging_post_with_failover(payload))
    kafka_started = time.perf_counter()
    kafka_task = asyncio.create_task(
        kafka_producer.send_and_wait(
            balance_updates_topic,
            value=payload,
            key=request.user_id,
        )
    )

    results = await asyncio.gather(log_task, kafka_task, return_exceptions=True)
    if isinstance(results[0], Exception):
        raise results[0]
    if isinstance(results[1], Exception):
        raise HTTPException(
            status_code=503, detail=f"Failed to publish to Kafka: {results[1]}"
        )

    _, logging_elapsed = results[0]
    record_metadata = results[1]
    kafka_elapsed = time.perf_counter() - kafka_started

    print(
        f"[{instance_id}] Published transaction tx={transaction_id} to Kafka "
        f"topic={record_metadata.topic} partition={record_metadata.partition} offset={record_metadata.offset}",
        flush=True,
    )

    global logging_time_total, counter_time_total
    async with metrics_lock:
        logging_time_total += logging_elapsed
        counter_time_total += kafka_elapsed

    return {
        "transaction_id": str(transaction_id),
        "user_id": request.user_id,
        "amount": request.amount,
        "queued": True,
        "kafka_offset": record_metadata.offset,
        "kafka_partition": record_metadata.partition,
    }


@app.get("/user/{user_id}")
async def get_user_balance(user_id: str):
    log_task = asyncio.create_task(logging_get_with_failover())
    counter_task = asyncio.create_task(counter_get_with_failover(f"/balance/{user_id}"))

    log_result, counter_result = await asyncio.gather(log_task, counter_task)
    log_response, logging_elapsed = log_result
    counter_data, counter_elapsed = counter_result

    user_transactions = [
        {
            "transaction_id": tx_id,
            "user_id": tx_data["user_id"],
            "amount": tx_data["amount"],
            "timestamp": tx_data["timestamp"],
        }
        for tx_id, tx_data in log_response.get("transactions", {}).items()
        if tx_data["user_id"] == user_id
    ]

    global logging_time_total, counter_time_total
    async with metrics_lock:
        logging_time_total += logging_elapsed
        counter_time_total += counter_elapsed

    balance = counter_data.get("balance") if counter_data is not None else None
    return {
        "user_id": user_id,
        "balance": balance,
        "transactions": user_transactions,
    }


@app.get("/accounts")
async def get_all_accounts():
    counter_data, counter_elapsed = await counter_get_with_failover("/accounts")

    global counter_time_total
    async with metrics_lock:
        counter_time_total += counter_elapsed

    if counter_data is None:
        return {"accounts": None}
    return counter_data


@app.get("/services")
async def get_services():
    """Convenience endpoint to inspect what facade currently sees in config-server."""
    if config_client is None:
        raise HTTPException(status_code=503, detail="config-server client not ready")
    response = await config_client._client.get(f"{config_client.base_url}/services")
    response.raise_for_status()
    return response.json()


@app.get("/metrics")
async def get_metrics():
    async with metrics_lock:
        return {
            "logging_time_total_sec": logging_time_total,
            "counter_kafka_publish_total_sec": counter_time_total,
        }


@app.post("/metrics/reset")
async def reset_metrics():
    global logging_time_total, counter_time_total
    async with metrics_lock:
        logging_time_total = 0.0
        counter_time_total = 0.0
    return {"message": "Metrics reset"}
