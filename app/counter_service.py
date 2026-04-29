import asyncio
import json
import os
import socket
from contextlib import asynccontextmanager

import asyncpg
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from kubernetes_client import KubernetesApiClient

database_url = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/counterdb"
)
service_name = os.getenv("SERVICE_NAME", "counter-service")
instance_id = os.getenv("INSTANCE_ID", os.getenv("POD_NAME", socket.gethostname()))
http_port = int(os.getenv("HTTP_PORT", "8001"))
configmap_name = os.getenv("APP_CONFIGMAP_NAME", "microservices-config")

db_pool: asyncpg.Pool | None = None
kafka_consumer: AIOKafkaConsumer | None = None
consumer_task: asyncio.Task | None = None
k8s_client: KubernetesApiClient | None = None


async def apply_balance_update(
    transaction_id: str, user_id: str, amount: float
) -> float:
    assert db_pool is not None
    async with db_pool.acquire() as connection:
        new_balance = await connection.fetchval(
            """
            INSERT INTO balances (user_id, balance)
            VALUES ($1, $2)
            ON CONFLICT (user_id)
            DO UPDATE SET balance = balances.balance + EXCLUDED.balance
            RETURNING balance
            """,
            user_id,
            amount,
        )
    print(
        f"[{instance_id}] Applied balance update tx={transaction_id} user={user_id} "
        f"amount={amount} -> new_balance={new_balance}",
        flush=True,
    )
    return new_balance


async def consume_balance_updates(
    bootstrap_servers: str, topic: str, group_id: str
) -> None:
    global kafka_consumer
    kafka_consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    last_error: BaseException | None = None
    for attempt in range(1, 61):
        try:
            await kafka_consumer.start()
            print(
                f"[{instance_id}] Kafka consumer started (attempt {attempt}): "
                f"bootstrap={bootstrap_servers} topic={topic} group={group_id}",
                flush=True,
            )
            break
        except (KafkaConnectionError, KafkaError, OSError) as exc:
            last_error = exc
            print(
                f"[{instance_id}] Kafka consumer start attempt {attempt} failed: {exc}. Retrying in 2s...",
                flush=True,
            )
            await asyncio.sleep(2.0)
    else:
        raise RuntimeError(
            f"Could not connect to Kafka after 60 attempts: {last_error}"
        )
    try:
        async for message in kafka_consumer:
            payload = message.value
            transaction_id = payload.get("transaction_id")
            user_id = payload.get("user_id")
            amount = payload.get("amount")
            print(
                f"[{instance_id}] Consumed Kafka message offset={message.offset} "
                f"partition={message.partition} key={message.key} value={payload}",
                flush=True,
            )
            if not transaction_id or not user_id or amount is None:
                print(
                    f"[{instance_id}] Skipping malformed message: {payload}", flush=True
                )
                await kafka_consumer.commit()
                continue

            try:
                await apply_balance_update(transaction_id, user_id, float(amount))
                await kafka_consumer.commit()
            except Exception as exc:
                print(
                    f"[{instance_id}] ERROR while applying tx={transaction_id}: {exc}. "
                    f"Will retry on next poll.",
                    flush=True,
                )
                await asyncio.sleep(1.0)
    finally:
        await kafka_consumer.stop()
        print(f"[{instance_id}] Kafka consumer stopped", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, consumer_task, k8s_client

    db_pool = await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=10)
    async with db_pool.acquire() as connection:
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS balances (
                user_id TEXT PRIMARY KEY,
                balance DOUBLE PRECISION NOT NULL DEFAULT 0
            )
            """)
    print(f"[{instance_id}] Database pool ready ({database_url})", flush=True)

    k8s_client = KubernetesApiClient()

    bootstrap_servers = await k8s_client.get_config_value_with_retry(
        configmap_name,
        "kafka.bootstrap.servers",
    )
    topic = await k8s_client.get_config_value_with_retry(
        configmap_name,
        "kafka.balance_updates.topic",
    )
    group_id = await k8s_client.get_config_value_with_retry(
        configmap_name,
        "kafka.balance_updates.group_id",
    )

    consumer_task = asyncio.create_task(
        consume_balance_updates(bootstrap_servers, topic, group_id)
    )

    yield

    print(f"[{instance_id}] Shutting down...", flush=True)
    if consumer_task is not None:
        consumer_task.cancel()
        try:
            await consumer_task
        except (asyncio.CancelledError, Exception):
            pass

    if k8s_client is not None:
        await k8s_client.close()

    if db_pool is not None:
        await db_pool.close()
    print(f"[{instance_id}] Shutdown complete", flush=True)


app = FastAPI(lifespan=lifespan)


class BalanceUpdateRequest(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not initialized")

    async with db_pool.acquire() as connection:
        balance = await connection.fetchval(
            "SELECT balance FROM balances WHERE user_id = $1",
            user_id,
        )

    return {"user_id": user_id, "balance": balance}


@app.get("/accounts")
async def get_all_accounts():
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not initialized")

    async with db_pool.acquire() as connection:
        rows = await connection.fetch("SELECT user_id, balance FROM balances")

    return {"accounts": {row["user_id"]: row["balance"] for row in rows}}


@app.get("/health")
async def health():
    return {"status": "ok", "instance_id": instance_id}
