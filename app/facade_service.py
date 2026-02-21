import asyncio
import datetime
import os
import time
import uuid
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI
from pydantic import BaseModel

logging_service_base_url = os.getenv("LOGGING_SERVICE_URL", "http://logging-service:8000")
counter_service_base_url = os.getenv("COUNTER_SERVICE_URL", "http://counter-service:8001")

logging_time_total = 0.0
counter_time_total = 0.0
metrics_lock = asyncio.Lock()
http_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=10.0)
    yield
    if http_client:
        await http_client.aclose()


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

    log_task = timed_post(
        http_client,
        f"{logging_service_base_url}/log",
        payload
    )
    counter_task = timed_post(
        http_client,
        f"{counter_service_base_url}/update_balance",
        payload
    )
    
    (log_response, logging_elapsed), (counter_response, counter_elapsed) = await asyncio.gather(
        log_task, counter_task
    )
    
    log_response.raise_for_status()
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
    log_task = timed_get(
        http_client,
        f"{logging_service_base_url}/logs"
    )
    counter_task = timed_get(
        http_client,
        f"{counter_service_base_url}/balance/{user_id}"
    )

    (log_response, logging_elapsed), (counter_response, counter_elapsed) = await asyncio.gather(
        log_task, counter_task
    )

    log_response.raise_for_status()
    counter_response.raise_for_status()

    log_data = log_response.json()
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
