import asyncio
import datetime
import itertools
import json
import os
import time
import uuid
from contextlib import asynccontextmanager

import grpc
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging_service_targets_env = os.getenv("LOGGING_SERVICE_TARGETS", "")

if logging_service_targets_env.strip():
    logging_service_targets = [target.strip() for target in logging_service_targets_env.split(",") if target.strip()]
else:
    logging_service_targets = ["logging-service-1:50051", "logging-service-2:50051", "logging-service-3:50051"]

counter_service_base_url = os.getenv("COUNTER_SERVICE_URL", "http://counter-service:8001")

logging_time_total = 0.0
counter_time_total = 0.0
metrics_lock = asyncio.Lock()
http_client = None
logging_grpc_clients = []
_grpc_round_robin = None


def json_dumps(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def json_loads(raw: bytes) -> dict:
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


class LoggingGrpcClient:
    def __init__(self, target: str):
        self.target = target
        self.channel = grpc.aio.insecure_channel(
            target,
            options=[
                ("grpc.max_send_message_length", 32 * 1024 * 1024),
                ("grpc.max_receive_message_length", 32 * 1024 * 1024),
            ]
        )
        self.log_transaction = self.channel.unary_unary(
            "/logging.LoggingService/LogTransaction",
            request_serializer=json_dumps,
            response_deserializer=json_loads,
        )
        self.get_logs = self.channel.unary_unary(
            "/logging.LoggingService/GetLogs",
            request_serializer=json_dumps,
            response_deserializer=json_loads,
        )

    async def close(self):
        await self.channel.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    global logging_grpc_clients
    global _grpc_round_robin
    print(f"Facade Service: Initializing gRPC clients for targets: {logging_service_targets}", flush=True)
    http_client = httpx.AsyncClient(timeout=10.0)
    logging_grpc_clients = [LoggingGrpcClient(target) for target in logging_service_targets]
    _grpc_round_robin = itertools.cycle(range(len(logging_grpc_clients)))
    print(f"Facade Service: Initialized {len(logging_grpc_clients)} gRPC clients", flush=True)
    yield
    for client in logging_grpc_clients:
        await client.close()
    if http_client:
        await http_client.aclose()
    print(f"Facade Service: Shutdown complete", flush=True)


app = FastAPI(lifespan=lifespan)


class TransactionRequest(BaseModel):
    user_id: str
    amount: float


async def timed_post(client, url, json):
    response = await client.post(url, json=json)
    elapsed = response.elapsed.total_seconds()
    return response, elapsed

async def timed_get(client, url):
    response = await client.get(url)
    elapsed = response.elapsed.total_seconds()
    return response, elapsed


def _ordered_clients():
    n = len(logging_grpc_clients)
    if n == 0:
        return []
    start = next(_grpc_round_robin)
    return [logging_grpc_clients[(start + i) % n] for i in range(n)]


async def logging_post_with_failover(payload: dict):
    last_error = None

    for grpc_client in _ordered_clients():
        try:
            started = time.perf_counter()
            response = await grpc_client.log_transaction(payload, timeout=10.0)
            elapsed = time.perf_counter() - started
            if response.get("status") == "ok":
                return response, elapsed
            raise RuntimeError(response.get("message", "Unknown gRPC logging error"))
        except (grpc.RpcError, RuntimeError) as error:
            last_error = error

    print(f"Facade: All logging-service instances unavailable: {last_error}", flush=True)
    raise HTTPException(status_code=503, detail=f"All logging-service instances unavailable: {last_error}")


async def logging_get_with_failover():
    last_error = None

    for grpc_client in _ordered_clients():
        try:
            started = time.perf_counter()
            response = await grpc_client.get_logs({}, timeout=10.0)
            elapsed = time.perf_counter() - started
            return response, elapsed
        except grpc.RpcError as error:
            last_error = error

    raise HTTPException(status_code=503, detail=f"All logging-service instances unavailable: {last_error}")

@app.post("/transaction")
async def process_transaction(request: TransactionRequest):
    timestamp = datetime.datetime.now()
    transaction_id = uuid.uuid4()
    payload = {
        "transaction_id": str(transaction_id),
        "user_id": request.user_id,
        "amount": request.amount,
        "timestamp": timestamp.isoformat()
    }

    log_task = logging_post_with_failover(payload)
    counter_task = timed_post(
        http_client,
        f"{counter_service_base_url}/update_balance",
        payload
    )
    
    results = await asyncio.gather(log_task, counter_task, return_exceptions=True)
    if isinstance(results[0], Exception):
        raise results[0]
    if isinstance(results[1], Exception):
        raise results[1]

    (_, logging_elapsed), (counter_response, counter_elapsed) = results
    counter_response.raise_for_status()

    counter_data = counter_response.json()

    global logging_time_total
    global counter_time_total
    async with metrics_lock:
        logging_time_total += logging_elapsed
        counter_time_total += counter_elapsed

    return {
        "transaction_id": str(transaction_id),
        "balance": counter_data.get("new_balance"),
    }

@app.get("/user/{user_id}")
async def get_user_balance(user_id: str):
    log_task = logging_get_with_failover()
    counter_task = timed_get(
        http_client,
        f"{counter_service_base_url}/balance/{user_id}"
    )

    (log_response, logging_elapsed), (counter_response, counter_elapsed) = await asyncio.gather(
        log_task, counter_task
    )

    counter_response.raise_for_status()

    log_data = log_response
    counter_data = counter_response.json()

    global logging_time_total
    global counter_time_total
    async with metrics_lock:
        logging_time_total += logging_elapsed
        counter_time_total += counter_elapsed

    user_transactions = [
        {
            "transaction_id": tx_id,
            "user_id": tx_data["user_id"],
            "amount": tx_data["amount"],
            "timestamp": tx_data["timestamp"],
        }
        for tx_id, tx_data in log_data.get("transactions", {}).items()
        if tx_data["user_id"] == user_id
    ]

    return {
        "user_id": user_id,
        "balance": counter_data.get("balance"),
        "transactions": user_transactions,
    }

@app.get("/accounts")
async def get_all_accounts():
    counter_response, counter_elapsed = await timed_get(
        http_client,
        f"{counter_service_base_url}/accounts"
    )
    counter_response.raise_for_status()
    counter_data = counter_response.json()

    global counter_time_total
    async with metrics_lock:
        counter_time_total += counter_elapsed

    return counter_data

@app.get("/metrics")
async def get_metrics():
    async with metrics_lock:
        return {
            "logging_time_total_sec": logging_time_total,
            "counter_time_total_sec": counter_time_total,
        }

@app.post("/metrics/reset")
async def reset_metrics():
    global logging_time_total
    global counter_time_total
    async with metrics_lock:
        logging_time_total = 0.0
        counter_time_total = 0.0
    return {"message": "Metrics reset"}
