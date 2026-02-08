import datetime
import os
import time
import requests
import uuid
from fastapi import FastAPI, Request
from pydantic import BaseModel

app = FastAPI()

logging_service_base_url = os.getenv("LOGGING_SERVICE_URL", "http://logging-service:8000")
counter_service_base_url = os.getenv("COUNTER_SERVICE_URL", "http://counter-service:8001")

logging_time_total = 0.0
counter_time_total = 0.0


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

    global logging_time_total
    global counter_time_total

    start_logging = time.perf_counter()
    log_response = requests.post(
        f"{logging_service_base_url}/log",
        json=payload,
        timeout=5,
    )
    log_response.raise_for_status()
    logging_time_total += time.perf_counter() - start_logging

    start_counter = time.perf_counter()
    counter_response = requests.post(
        f"{counter_service_base_url}/update_balance",
        json=payload,
        timeout=5,
    )
    counter_response.raise_for_status()
    counter_time_total += time.perf_counter() - start_counter

    counter_data = counter_response.json()
    return {
        "transaction_id": str(transaction_id),
        "balance": counter_data.get("new_balance"),
    }

@app.get("/user/{user_id}")
async def get_user_balance(user_id: str):
    global logging_time_total
    global counter_time_total

    start_logging = time.perf_counter()
    log_response = requests.get(
        f"{logging_service_base_url}/logs",
        timeout=5,
    )
    log_response.raise_for_status()
    logging_time_total += time.perf_counter() - start_logging
    log_data = log_response.json()

    start_counter = time.perf_counter()
    counter_response = requests.get(
        f"{counter_service_base_url}/balance/{user_id}",
        timeout=5,
    )
    counter_response.raise_for_status()
    counter_time_total += time.perf_counter() - start_counter
    counter_data = counter_response.json()

    user_transactions = [
        tx for tx in log_data.get("transactions", {}).values() if tx["user_id"] == user_id
    ]

    return {
        "user_id": user_id,
        "balance": counter_data.get("balance"),
        "transactions": user_transactions,
    }

@app.get("/accounts")
async def get_all_accounts():
    global counter_time_total

    start_counter = time.perf_counter()
    counter_response = requests.get(
        f"{counter_service_base_url}/accounts",
        timeout=5,
    )
    counter_response.raise_for_status()
    counter_time_total += time.perf_counter() - start_counter
    counter_data = counter_response.json()
    return counter_data

@app.get("/metrics")
async def get_metrics():
    return {
        "logging_time_total_sec": logging_time_total,
        "counter_time_total_sec": counter_time_total,
    }

@app.post("/metrics/reset")
async def reset_metrics():
    global logging_time_total
    global counter_time_total
    logging_time_total = 0.0
    counter_time_total = 0.0
    return {"message": "Metrics reset"}
