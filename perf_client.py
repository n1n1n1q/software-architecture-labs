import argparse
import asyncio
import time
from typing import Dict

import httpx
from tqdm.asyncio import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Facade performance client")
    parser.add_argument("--base-url", default="http://localhost:8002", help="Facade base URL")
    parser.add_argument("--clients", type=int, default=10, help="Number of concurrent clients")
    parser.add_argument("--requests-per-client", type=int, default=10000, help="Requests per client")
    parser.add_argument("--amount", type=float, default=1.0, help="Amount per transaction")
    parser.add_argument("--same-user", action="store_true", help="Use same user for all clients")
    parser.add_argument("--verify", action="store_true", help="Verify balances after run")
    return parser.parse_args()


async def post_transactions(
    client: httpx.AsyncClient,
    base_url: str,
    user_id: str,
    amount: float,
    count: int,
    progress: tqdm,
) -> None:
    url = f"{base_url}/transaction"
    payload = {"user_id": user_id, "amount": amount}
    for _ in range(count):
        response = await client.post(url, json=payload, timeout=30.0)
        response.raise_for_status()
        progress.update(1)


async def fetch_metrics(client: httpx.AsyncClient, base_url: str) -> Dict[str, float]:
    response = await client.get(f"{base_url}/metrics", timeout=30.0)
    response.raise_for_status()
    return response.json()


async def reset_metrics(client: httpx.AsyncClient, base_url: str) -> None:
    response = await client.post(f"{base_url}/metrics/reset", timeout=30.0)
    response.raise_for_status()


async def fetch_accounts(client: httpx.AsyncClient, base_url: str) -> Dict[str, float]:
    response = await client.get(f"{base_url}/accounts", timeout=30.0)
    response.raise_for_status()
    return response.json().get("accounts", {})


async def fetch_user(client: httpx.AsyncClient, base_url: str, user_id: str) -> Dict[str, float]:
    response = await client.get(f"{base_url}/user/{user_id}", timeout=30.0)
    response.raise_for_status()
    return response.json()


async def main() -> None:
    args = parse_args()

    total_requests = args.clients * args.requests_per_client
    user_ids = ["user-0" if args.same_user else f"user-{i}" for i in range(args.clients)]

    async with httpx.AsyncClient() as client:
        await reset_metrics(client, args.base_url)

        progress = tqdm(total=total_requests, desc="Requests", unit="req")
        try:
            start = time.perf_counter()
            tasks = []
            for i in range(args.clients):
                task = post_transactions(
                    client,
                    args.base_url,
                    user_ids[i],
                    args.amount,
                    args.requests_per_client,
                    progress,
                )
                tasks.append(task)
            
            await asyncio.gather(*tasks)
            elapsed = time.perf_counter() - start
        finally:
            progress.close()

        rps = total_requests / elapsed if elapsed > 0 else 0.0
        metrics = await fetch_metrics(client, args.base_url)

        print("Total requests:", total_requests)
        print("Elapsed seconds:", round(elapsed, 4))
        print("Requests per second:", round(rps, 2))
        print("Logging total seconds:", round(metrics.get("logging_time_total_sec", 0.0), 4))
        print("Counter total seconds:", round(metrics.get("counter_time_total_sec", 0.0), 4))

        if args.verify:
            if args.same_user:
                user = await fetch_user(client, args.base_url, "user-0")
                print("User balance:", user.get("balance"))
            else:
                accounts = await fetch_accounts(client, args.base_url)
                print("Accounts:", accounts)


if __name__ == "__main__":
    asyncio.run(main())
