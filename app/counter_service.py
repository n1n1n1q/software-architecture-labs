import asyncio
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

balances = {}
balances_lock = asyncio.Lock()

class BalanceUpdateRequest(BaseModel):
    transaction_id: str
    user_id: str
    amount: float

@app.post("/update_balance")
async def update_balance(update: BalanceUpdateRequest):
    user_id = update.user_id
    amount = update.amount
    async with balances_lock:
        if user_id not in balances:
            balances[user_id] = 0.0
        balances[user_id] += amount
        new_balance = balances[user_id]
    return {"user_id": user_id, "new_balance": new_balance}

@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    async with balances_lock:
        balance = balances.get(user_id, 0.0)
    return {"user_id": user_id, "balance": balance}

@app.get("/accounts")
async def get_all_accounts():
    async with balances_lock:
        accounts_copy = dict(balances)
    return {"accounts": accounts_copy}