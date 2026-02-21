import asyncio
from pydantic import BaseModel
from fastapi import FastAPI

app = FastAPI()

class LogRequest(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    timestamp: str

transactions = {}
transactions_lock = asyncio.Lock()

@app.post("/log")
async def log_transaction(log_request: LogRequest):
    async with transactions_lock:
        transactions[log_request.transaction_id] = {
            "user_id": log_request.user_id,
            "amount": log_request.amount,
            "timestamp": log_request.timestamp,
        }
        print(f"Logged transaction: {log_request.transaction_id}")
    return {"message": "Transaction logged successfully"}

@app.get("/logs")
async def get_logs():
    async with transactions_lock:
        transactions_copy = dict(transactions)
    return {"transactions": transactions_copy}
