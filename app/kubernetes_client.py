from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import httpx

SERVICE_ACCOUNT_DIR = Path("/var/run/secrets/kubernetes.io/serviceaccount")


@dataclass(frozen=True)
class ServiceInstance:
    ip: str
    port: int
    ready: bool
    pod_name: Optional[str] = None

    @property
    def host_port(self) -> str:
        return f"{self.ip}:{self.port}"


class KubernetesApiClient:
    def __init__(
        self,
        namespace: Optional[str] = None,
        timeout: float = 5.0,
    ) -> None:
        api_host = os.getenv("KUBERNETES_SERVICE_HOST")
        api_port = os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not api_host:
            raise RuntimeError(
                "KUBERNETES_SERVICE_HOST is not set. This service must run inside Kubernetes."
            )

        token = self._read_required_file(SERVICE_ACCOUNT_DIR / "token")
        ca_cert_path = SERVICE_ACCOUNT_DIR / "ca.crt"
        if not ca_cert_path.exists():
            raise RuntimeError(
                "Kubernetes CA certificate not found in service account directory."
            )

        if namespace is None:
            namespace = os.getenv("POD_NAMESPACE")
        if namespace is None:
            namespace = self._read_required_file(SERVICE_ACCOUNT_DIR / "namespace")

        self.namespace = namespace
        self._client = httpx.AsyncClient(
            base_url=f"https://{api_host}:{api_port}",
            headers={"Authorization": f"Bearer {token}"},
            verify=str(ca_cert_path),
            timeout=timeout,
        )

    @staticmethod
    def _read_required_file(path: Path) -> str:
        try:
            return path.read_text(encoding="utf-8").strip()
        except FileNotFoundError as exc:
            raise RuntimeError(f"Required file not found: {path}") from exc

    async def close(self) -> None:
        await self._client.aclose()

    async def _get_json(self, path: str) -> dict:
        response = await self._client.get(path)
        response.raise_for_status()
        return response.json()

    async def get_configmap_data(self, configmap_name: str) -> dict[str, str]:
        namespace = quote(self.namespace, safe="")
        name = quote(configmap_name, safe="")
        path = f"/api/v1/namespaces/{namespace}/configmaps/{name}"
        payload = await self._get_json(path)
        return payload.get("data", {})

    async def get_config_value(self, configmap_name: str, key: str) -> str:
        data = await self.get_configmap_data(configmap_name)
        if key not in data:
            raise KeyError(
                f"Config key '{key}' was not found in ConfigMap '{configmap_name}'"
            )
        return data[key]

    async def get_config_value_with_retry(
        self,
        configmap_name: str,
        key: str,
        max_attempts: int = 60,
        retry_delay: float = 2.0,
    ) -> str:
        last_error: Optional[BaseException] = None
        for attempt in range(1, max_attempts + 1):
            try:
                return await self.get_config_value(configmap_name, key)
            except (httpx.HTTPError, OSError, KeyError) as exc:
                last_error = exc
                print(
                    f"[k8s] Failed to fetch '{key}' from ConfigMap '{configmap_name}' "
                    f"on attempt {attempt}: {exc}",
                    flush=True,
                )
                await asyncio.sleep(retry_delay)
        raise RuntimeError(
            f"Could not fetch '{key}' from ConfigMap '{configmap_name}' "
            f"after {max_attempts} attempts: {last_error}"
        )

    async def get_service_instances(
        self,
        service_name: str,
        port_name: Optional[str] = None,
    ) -> list[ServiceInstance]:
        namespace = quote(self.namespace, safe="")
        name = quote(service_name, safe="")
        path = f"/api/v1/namespaces/{namespace}/endpoints/{name}"
        response = await self._client.get(path)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        payload = response.json()
        subsets = payload.get("subsets", [])

        instances: dict[tuple[str, int], ServiceInstance] = {}
        for subset in subsets:
            ports = subset.get("ports", [])
            port_value: Optional[int] = None

            if port_name is not None:
                for port in ports:
                    if port.get("name") == port_name:
                        port_value = int(port.get("port"))
                        break
            elif ports:
                port_value = int(ports[0].get("port"))

            if port_value is None:
                continue

            def upsert(address: dict, ready: bool) -> None:
                ip = address.get("ip")
                if not ip:
                    return
                pod_name = None
                target_ref = address.get("targetRef") or {}
                if target_ref.get("kind") == "Pod":
                    pod_name = target_ref.get("name")
                key = (ip, port_value)
                existing = instances.get(key)
                candidate = ServiceInstance(
                    ip=ip,
                    port=port_value,
                    ready=ready,
                    pod_name=pod_name,
                )
                if existing is None or (candidate.ready and not existing.ready):
                    instances[key] = candidate

            for address in subset.get("addresses", []):
                upsert(address, ready=True)
            for address in subset.get("notReadyAddresses", []):
                upsert(address, ready=False)

        return list(instances.values())

    async def discover_service_addresses(
        self,
        service_name: str,
        port_name: Optional[str] = None,
        scheme: Optional[str] = None,
        ready_only: bool = True,
    ) -> list[str]:
        instances = await self.get_service_instances(service_name, port_name=port_name)
        if ready_only:
            instances = [instance for instance in instances if instance.ready]

        if scheme is None:
            return [instance.host_port for instance in instances]
        return [f"{scheme}://{instance.host_port}" for instance in instances]
