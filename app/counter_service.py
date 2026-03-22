import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/counterdb")
db_pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool

    db_pool = await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=10)
    async with db_pool.acquire() as connection:
        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS balances (
                user_id TEXT PRIMARY KEY,
                balance DOUBLE PRECISION NOT NULL DEFAULT 0
            )
            """
        )

    yield

    if db_pool is not None:
        await db_pool.close()


app = FastAPI(lifespan=lifespan)

class BalanceUpdateRequest(BaseModel):
    transaction_id: str
    user_id: str
    amount: float

@app.post("/update_balance")
async def update_balance(update: BalanceUpdateRequest):
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not initialized")

    user_id = update.user_id
    amount = update.amount
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

    return {"user_id": user_id, "new_balance": new_balance}


@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not initialized")

    async with db_pool.acquire() as connection:
        balance = await connection.fetchval(
            "SELECT balance FROM balances WHERE user_id = $1",
            user_id,
        )

    if balance is None:
        balance = 0.0

    return {"user_id": user_id, "balance": balance}


@app.get("/accounts")
async def get_all_accounts():
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not initialized")

    async with db_pool.acquire() as connection:
        rows = await connection.fetch("SELECT user_id, balance FROM balances")

    accounts_copy = {row["user_id"]: row["balance"] for row in rows}

    return {"accounts": accounts_copy}