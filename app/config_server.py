import os
from collections import defaultdict
from contextlib import asynccontextmanager
from threading import RLock
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

registry: dict[str, dict[str, str]] = defaultdict(dict)
registry_lock = RLock()

static_config: dict[str, str] = {
    "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    "kafka.balance_updates.topic": os.getenv(
        "KAFKA_BALANCE_UPDATES_TOPIC", "balance-updates"
    ),
    "kafka.balance_updates.group_id": os.getenv(
        "KAFKA_BALANCE_UPDATES_GROUP", "counter-service"
    ),
    "hazelcast.cluster_name": os.getenv("HAZELCAST_CLUSTER_NAME", "hello-world"),
    "hazelcast.members": os.getenv(
        "HAZELCAST_MEMBERS",
        "hz-node1:5701,hz-node2:5701,hz-node3:5701",
    ),
    "hazelcast.map_name": os.getenv("HAZELCAST_MAP_NAME", "transactions-map"),
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[config-server] Starting with static config: {static_config}", flush=True)
    yield
    print("[config-server] Shutdown complete", flush=True)


app = FastAPI(lifespan=lifespan)


class RegisterRequest(BaseModel):
    service_name: str
    instance_id: str
    address: str


class UnregisterRequest(BaseModel):
    service_name: str
    instance_id: str


@app.post("/register")
async def register_service(request: RegisterRequest):
    with registry_lock:
        registry[request.service_name][request.instance_id] = request.address
        snapshot = dict(registry[request.service_name])
    print(
        f"[config-server] Registered {request.service_name}/{request.instance_id} -> {request.address}. "
        f"Current instances: {snapshot}",
        flush=True,
    )
    return {"status": "ok", "service_name": request.service_name, "instances": snapshot}


@app.post("/unregister")
async def unregister_service(request: UnregisterRequest):
    with registry_lock:
        instances = registry.get(request.service_name, {})
        removed = instances.pop(request.instance_id, None)
        snapshot = dict(instances)
    if removed is None:
        raise HTTPException(status_code=404, detail="instance not found")
    print(
        f"[config-server] Unregistered {request.service_name}/{request.instance_id}. "
        f"Remaining: {snapshot}",
        flush=True,
    )
    return {"status": "ok", "service_name": request.service_name, "instances": snapshot}


@app.get("/services/{service_name}")
async def get_service(service_name: str):
    with registry_lock:
        instances = dict(registry.get(service_name, {}))
    return {"service_name": service_name, "instances": instances}


@app.get("/services")
async def list_services():
    with registry_lock:
        snapshot = {name: dict(instances) for name, instances in registry.items()}
    return {"services": snapshot}


@app.get("/config/{key:path}")
async def get_config(key: str):
    if key not in static_config:
        raise HTTPException(status_code=404, detail=f"config key '{key}' not found")
    return {"key": key, "value": static_config[key]}


@app.get("/config")
async def get_all_config():
    return {"config": dict(static_config)}
