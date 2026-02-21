import asyncio
import datetime
import os
import time
import uuid
import httpx
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

logging_service_base_url = os.getenv("LOGGING_SERVICE_URL", "http://logging-service:8000")
counter_service_base_url = os.getenv("COUNTER_SERVICE_URL", "http://counter-service:8001")

logging_time_total = 0.0
counter_time_total = 0.0
metrics_lock = asyncio.Lock()


class TransactionRequest(BaseModel):
    user_id: str
    amount: float


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

    async with httpx.AsyncClient() as client:
        start_logging = time.perf_counter()
        start_counter = time.perf_counter()
        
        log_task = client.post(
            f"{logging_service_base_url}/log",
            json=payload,
            timeout=10.0,
        )
        counter_task = client.post(
            f"{counter_service_base_url}/update_balance",
            json=payload,
            timeout=10.0,
        )
        
        log_response, counter_response = await asyncio.gather(log_task, counter_task)
        
        log_response.raise_for_status()
        counter_response.raise_for_status()
        
        logging_elapsed = time.perf_counter() - start_logging
        counter_elapsed = time.perf_counter() - start_counter

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
    async with httpx.AsyncClient() as client:
        log_task = client.get(
            f"{logging_service_base_url}/logs",
            timeout=10.0,
        )
        counter_task = client.get(
            f"{counter_service_base_url}/balance/{user_id}",
            timeout=10.0,
        )

        start_logging = time.perf_counter()
        log_response = await log_task
        log_response.raise_for_status()
        logging_elapsed = time.perf_counter() - start_logging

        start_counter = time.perf_counter()
        counter_response = await counter_task
        counter_response.raise_for_status()
        counter_elapsed = time.perf_counter() - start_counter

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
    async with httpx.AsyncClient() as client:
        start_counter = time.perf_counter()
        counter_response = await client.get(
            f"{counter_service_base_url}/accounts",
            timeout=10.0,
        )
        counter_response.raise_for_status()
        counter_elapsed = time.perf_counter() - start_counter
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
