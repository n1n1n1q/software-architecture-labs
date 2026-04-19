from __future__ import annotations

import asyncio
import os
import random
from typing import Optional

import httpx


class ConfigServerClient:
    def __init__(self, base_url: Optional[str] = None, timeout: float = 5.0):
        self.base_url = (
            base_url or os.getenv("CONFIG_SERVER_URL", "http://config-server:8500")
        ).rstrip("/")
        self.timeout = timeout
        self._client = httpx.AsyncClient(timeout=timeout)

    async def close(self) -> None:
        await self._client.aclose()

    async def register(
        self,
        service_name: str,
        instance_id: str,
        address: str,
        max_attempts: int = 60,
        retry_delay: float = 2.0,
    ) -> None:
        payload = {
            "service_name": service_name,
            "instance_id": instance_id,
            "address": address,
        }
        last_error: Optional[BaseException] = None
        for attempt in range(1, max_attempts + 1):
            try:
                response = await self._client.post(
                    f"{self.base_url}/register", json=payload
                )
                response.raise_for_status()
                print(
                    f"[service-registry] Registered {service_name}/{instance_id} -> {address} "
                    f"on {self.base_url} (attempt {attempt})",
                    flush=True,
                )
                return
            except (httpx.HTTPError, OSError) as exc:
                last_error = exc
                print(
                    f"[service-registry] Failed to register {service_name}/{instance_id} on attempt {attempt}: {exc}",
                    flush=True,
                )
                await asyncio.sleep(retry_delay)
        raise RuntimeError(
            f"Could not register {service_name}/{instance_id} with config-server after {max_attempts} attempts: {last_error}"
        )

    async def unregister(self, service_name: str, instance_id: str) -> None:
        try:
            await self._client.post(
                f"{self.base_url}/unregister",
                json={"service_name": service_name, "instance_id": instance_id},
            )
            print(
                f"[service-registry] Unregistered {service_name}/{instance_id}",
                flush=True,
            )
        except httpx.HTTPError as exc:
            print(
                f"[service-registry] Failed to unregister {service_name}/{instance_id}: {exc}",
                flush=True,
            )

    async def discover(self, service_name: str) -> list[str]:
        response = await self._client.get(f"{self.base_url}/services/{service_name}")
        response.raise_for_status()
        instances = response.json().get("instances", {})
        return list(instances.values())

    async def discover_random(self, service_name: str) -> Optional[str]:
        addresses = await self.discover(service_name)
        if not addresses:
            return None
        return random.choice(addresses)

    async def get_config(self, key: str) -> str:
        response = await self._client.get(f"{self.base_url}/config/{key}")
        response.raise_for_status()
        return response.json()["value"]

    async def get_config_with_retry(
        self,
        key: str,
        max_attempts: int = 60,
        retry_delay: float = 2.0,
    ) -> str:
        last_error: Optional[BaseException] = None
        for attempt in range(1, max_attempts + 1):
            try:
                return await self.get_config(key)
            except (httpx.HTTPError, OSError) as exc:
                last_error = exc
                print(
                    f"[service-registry] Failed to fetch config '{key}' on attempt {attempt}: {exc}",
                    flush=True,
                )
                await asyncio.sleep(retry_delay)
        raise RuntimeError(
            f"Could not fetch config '{key}' from config-server after {max_attempts} attempts: {last_error}"
        )
