import argparse
import concurrent.futures
import time
from typing import Dict
import threading

import requests
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Facade performance client")
    parser.add_argument("--base-url", default="http://localhost:8002", help="Facade base URL")
    parser.add_argument("--clients", type=int, default=10, help="Number of concurrent clients")
    parser.add_argument("--requests-per-client", type=int, default=10000, help="Requests per client")
    parser.add_argument("--amount", type=float, default=1.0, help="Amount per transaction")
    parser.add_argument("--same-user", action="store_true", help="Use same user for all clients")
    parser.add_argument("--verify", action="store_true", help="Verify balances after run")
    return parser.parse_args()


def post_transactions(
    base_url: str,
    user_id: str,
    amount: float,
    count: int,
    progress: tqdm,
) -> None:
    session = requests.Session()
    url = f"{base_url}/transaction"
    payload = {"user_id": user_id, "amount": amount}
    for _ in range(count):
        response = session.post(url, json=payload, timeout=10)
        response.raise_for_status()
        progress.update(1)


def fetch_metrics(base_url: str) -> Dict[str, float]:
    response = requests.get(f"{base_url}/metrics", timeout=10)
    response.raise_for_status()
    return response.json()


def reset_metrics(base_url: str) -> None:
    response = requests.post(f"{base_url}/metrics/reset", timeout=10)
    response.raise_for_status()


def fetch_accounts(base_url: str) -> Dict[str, float]:
    response = requests.get(f"{base_url}/accounts", timeout=10)
    response.raise_for_status()
    return response.json().get("accounts", {})


def fetch_user(base_url: str, user_id: str) -> Dict[str, float]:
    response = requests.get(f"{base_url}/user/{user_id}", timeout=10)
    response.raise_for_status()
    return response.json()


def main() -> None:
    args = parse_args()

    total_requests = args.clients * args.requests_per_client
    user_ids = ["user-0" if args.same_user else f"user-{i}" for i in range(args.clients)]

    reset_metrics(args.base_url)

    tqdm.set_lock(threading.RLock())
    progress = tqdm(total=total_requests, desc="Requests", unit="req")
    try:
        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as executor:
            futures = []
            for i in range(args.clients):
                futures.append(
                    executor.submit(
                        post_transactions,
                        args.base_url,
                        user_ids[i],
                        args.amount,
                        args.requests_per_client,
                        progress,
                    )
                )
            for future in futures:
                future.result()
        elapsed = time.perf_counter() - start
    finally:
        progress.close()

    rps = total_requests / elapsed if elapsed > 0 else 0.0
    metrics = fetch_metrics(args.base_url)

    print("Total requests:", total_requests)
    print("Elapsed seconds:", round(elapsed, 4))
    print("Requests per second:", round(rps, 2))
    print("Logging total seconds:", round(metrics.get("logging_time_total_sec", 0.0), 4))
    print("Counter total seconds:", round(metrics.get("counter_time_total_sec", 0.0), 4))

    if args.verify:
        if args.same_user:
            user = fetch_user(args.base_url, "user-0")
            print("User balance:", user.get("balance"))
        else:
            accounts = fetch_accounts(args.base_url)
            print("Accounts:", accounts)


if __name__ == "__main__":
    main()
