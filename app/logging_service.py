import json
import os
import socket
from contextlib import asynccontextmanager

import grpc
import hazelcast
from fastapi import FastAPI
from pydantic import BaseModel

from service_registry import ConfigServerClient

service_name = os.getenv("SERVICE_NAME", "logging-service")
instance_id = os.getenv("LOGGING_INSTANCE_ID", socket.gethostname())
grpc_port = int(os.getenv("LOGGING_GRPC_PORT", "50051"))
self_address = os.getenv("SELF_ADDRESS", f"{instance_id}:{grpc_port}")

hazelcast_map_name_default = os.getenv("HAZELCAST_MAP_NAME", "transactions-map")

hz_client = None
hz_map = None
grpc_server = None
config_client: ConfigServerClient | None = None


def encode_payload(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def decode_payload(raw: bytes) -> dict:
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


async def grpc_log_transaction(request: dict, context):
    print(
        f"[{instance_id}] gRPC LogTransaction called with request: {request}",
        flush=True,
    )
    if hz_map is None:
        print(f"[{instance_id}] ERROR: Hazelcast map is not initialized", flush=True)
        context.abort(grpc.StatusCode.UNAVAILABLE, "Hazelcast map is not initialized")

    transaction_id = request.get("transaction_id")
    user_id = request.get("user_id")
    amount = request.get("amount")
    timestamp = request.get("timestamp")

    if not transaction_id or not user_id or amount is None or not timestamp:
        print(f"[{instance_id}] ERROR: Missing required fields in request", flush=True)
        context.abort(
            grpc.StatusCode.INVALID_ARGUMENT,
            "transaction_id, user_id, amount, timestamp are required",
        )

    payload = {"user_id": user_id, "amount": amount, "timestamp": timestamp}
    hz_map.put(transaction_id, json.dumps(payload, separators=(",", ":")))
    print(
        f"[{instance_id}] Logged transaction via gRPC: {transaction_id} (user_id={user_id}, amount={amount})",
        flush=True,
    )

    return {
        "status": "ok",
        "instance_id": instance_id,
        "message": "Transaction logged successfully",
    }


async def grpc_get_logs(request: dict, context):
    print(f"[{instance_id}] gRPC GetLogs called", flush=True)
    if hz_map is None:
        print(f"[{instance_id}] ERROR: Hazelcast map is not initialized", flush=True)
        context.abort(grpc.StatusCode.UNAVAILABLE, "Hazelcast map is not initialized")

    transactions = hz_map.entry_set()
    transactions_copy = {
        tx_id: json.loads(raw_value) for tx_id, raw_value in transactions
    }

    print(
        f"[{instance_id}] Returned {len(transactions_copy)} transactions via gRPC",
        flush=True,
    )
    return {"transactions": transactions_copy, "instance_id": instance_id}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, hz_map, grpc_server, config_client

    config_client = ConfigServerClient()

    hazelcast_cluster_name = await config_client.get_config_with_retry(
        "hazelcast.cluster_name"
    )
    hazelcast_members_raw = await config_client.get_config_with_retry(
        "hazelcast.members"
    )
    hazelcast_map_name = hazelcast_map_name_default
    try:
        hazelcast_map_name = await config_client.get_config("hazelcast.map_name")
    except Exception as exc:
        print(
            f"[{instance_id}] WARN: could not fetch hazelcast.map_name from config-server: {exc}",
            flush=True,
        )

    cluster_members = [m.strip() for m in hazelcast_members_raw.split(",") if m.strip()]
    print(
        f"[{instance_id}] Connecting to Hazelcast cluster '{hazelcast_cluster_name}' members={cluster_members}",
        flush=True,
    )
    hz_client = hazelcast.HazelcastClient(
        cluster_name=hazelcast_cluster_name,
        cluster_members=cluster_members,
    )
    hz_map = hz_client.get_map(hazelcast_map_name).blocking()
    print(
        f"[{instance_id}] Connected to Hazelcast (map={hazelcast_map_name})", flush=True
    )

    print(f"[{instance_id}] Starting gRPC server on 0.0.0.0:{grpc_port}...", flush=True)
    grpc_server = grpc.aio.server(
        options=[
            ("grpc.max_send_message_length", 32 * 1024 * 1024),
            ("grpc.max_receive_message_length", 32 * 1024 * 1024),
        ]
    )
    grpc_handler = grpc.method_handlers_generic_handler(
        "logging.LoggingService",
        {
            "LogTransaction": grpc.unary_unary_rpc_method_handler(
                grpc_log_transaction,
                request_deserializer=decode_payload,
                response_serializer=encode_payload,
            ),
            "GetLogs": grpc.unary_unary_rpc_method_handler(
                grpc_get_logs,
                request_deserializer=decode_payload,
                response_serializer=encode_payload,
            ),
        },
    )
    grpc_server.add_generic_rpc_handlers((grpc_handler,))
    grpc_server.add_insecure_port(f"[::]:{grpc_port}")
    await grpc_server.start()
    print(f"[{instance_id}] gRPC server listening on 0.0.0.0:{grpc_port}", flush=True)

    await config_client.register(service_name, instance_id, self_address)

    yield

    print(f"[{instance_id}] Shutting down ...", flush=True)
    if config_client is not None:
        await config_client.unregister(service_name, instance_id)
        await config_client.close()
    if grpc_server is not None:
        await grpc_server.stop(5)
    if hz_client is not None:
        hz_client.shutdown()
    print(f"[{instance_id}] Shutdown complete", flush=True)


app = FastAPI(lifespan=lifespan)


class LogRequest(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    timestamp: str


@app.get("/health")
async def health():
    return {"status": "ok", "instance_id": instance_id}
