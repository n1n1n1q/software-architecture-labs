from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

balances = {}

class BalanceUpdateRequest(BaseModel):
    transaction_id: str
    user_id: str
    amount: float

@app.post("/update_balance")
async def update_balance(update: BalanceUpdateRequest):
    user_id = update.user_id
    amount = update.amount
    if user_id not in balances:
        balances[user_id] = 0.0
    balances[user_id] += amount
    return {"user_id": user_id, "new_balance": balances[user_id]}

@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    return {"user_id": user_id, "balance": balances.get(user_id, 0.0)}

@app.get("/accounts")
async def get_all_accounts():
    return {"accounts": balances}