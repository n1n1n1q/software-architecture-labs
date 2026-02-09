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
