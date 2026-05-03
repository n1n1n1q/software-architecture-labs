import asyncio
import json
import os
import socket
import sys
from contextlib import asynccontextmanager

import grpc
import hazelcast
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


def hz_future_to_asyncio(hz_future):
    loop = asyncio.get_running_loop()
    aio_future = loop.create_future()

    def _on_done(future):
        try:
            result = future.result()
        except BaseException as exc:
            loop.call_soon_threadsafe(
                lambda: aio_future.set_exception(exc) if not aio_future.done() else None
            )
        else:
            loop.call_soon_threadsafe(
                lambda: aio_future.set_result(result) if not aio_future.done() else None
            )

    hz_future.add_done_callback(_on_done)
    return aio_future


hazelcast_cluster_name = os.getenv("HAZELCAST_CLUSTER_NAME", "hello-world")
hazelcast_members = os.getenv(
    "HAZELCAST_MEMBERS",
    "hz-node1:5701,hz-node2:5701,hz-node3:5701",
)
hazelcast_map_name = os.getenv("HAZELCAST_MAP_NAME", "transactions-map")
instance_id = os.getenv("LOGGING_INSTANCE_ID", socket.gethostname())
grpc_port = int(os.getenv("LOGGING_GRPC_PORT", "50051"))

hz_client = None
hz_map = None
grpc_server = None


def encode_payload(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def decode_payload(raw: bytes) -> dict:
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


async def grpc_log_transaction(request: dict, context):
    if hz_map is None:
        context.abort(grpc.StatusCode.UNAVAILABLE, "Hazelcast map is not initialized")

    transaction_id = request.get("transaction_id")
    user_id = request.get("user_id")
    amount = request.get("amount")
    timestamp = request.get("timestamp")

    if not transaction_id or not user_id or amount is None or not timestamp:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "transaction_id, user_id, amount, timestamp are required")

    payload = {
        "user_id": user_id,
        "amount": amount,
        "timestamp": timestamp,
    }
    await hz_future_to_asyncio(
        hz_map.put(transaction_id, json.dumps(payload, separators=(",", ":")))
    )

    return {
        "status": "ok",
        "instance_id": instance_id,
        "message": "Transaction logged successfully",
    }


async def grpc_get_logs(request: dict, context):
    if hz_map is None:
        context.abort(grpc.StatusCode.UNAVAILABLE, "Hazelcast map is not initialized")

    transactions = await hz_future_to_asyncio(hz_map.entry_set())
    transactions_copy = {
        tx_id: json.loads(raw_value)
        for tx_id, raw_value in transactions
    }

    return {
        "transactions": transactions_copy,
        "instance_id": instance_id,
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client
    global hz_map
    global grpc_server

    try:
        print(f"[{instance_id}] Starting Hazelcast connection...", flush=True)
        cluster_members = [member.strip() for member in hazelcast_members.split(",") if member.strip()]
        print(f"[{instance_id}] Cluster members: {cluster_members}", flush=True)
        
        hz_client = hazelcast.HazelcastClient(
            cluster_name=hazelcast_cluster_name,
            cluster_members=cluster_members,
        )
        hz_map = hz_client.get_map(hazelcast_map_name)

        print(
            f"[{instance_id}] Connected to Hazelcast cluster '{hazelcast_cluster_name}' "
            f"with members: {cluster_members}",
            flush=True
        )
    except Exception as e:
        print(f"[{instance_id}] ERROR: Failed to connect to Hazelcast: {e}", flush=True)
        raise

    try:
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
    except Exception as e:
        print(f"[{instance_id}] ERROR: Failed to start gRPC server: {e}", flush=True)
        raise

    yield

    print(f"[{instance_id}] Shutting down...", flush=True)
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
