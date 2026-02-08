from pydantic import BaseModel
from fastapi import FastAPI, Request
import requests
app = FastAPI()

class LogRequest(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    timestamp: str

transactions = {}

@app.post("/log")
async def log_transaction(log_request: LogRequest):
    transactions[log_request.transaction_id] = {
        "user_id": log_request.user_id,
        "amount": log_request.amount,
        "timestamp": log_request.timestamp,
    }
    print(transactions[log_request.transaction_id])
    # requests.post
    return {"message": "Transaction logged successfully"}

@app.get("/logs")
async def get_logs():
    return {"transactions": transactions}

@app.get("/logs/user/{user_id}")
async def get_logs_for_user(user_id: str):
    user_transactions = []
    for transaction_id, data in transactions.items():
        if data.get("user_id") == user_id:
            user_transactions.append({
                "transaction_id": transaction_id,
                "user_id": data.get("user_id"),
                "amount": data.get("amount"),
                "timestamp": data.get("timestamp"),
            })
    return {"transactions": user_transactions}
