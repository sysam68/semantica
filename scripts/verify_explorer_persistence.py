#!/usr/bin/env python3
"""Exercise Explorer durability and interruption recovery against Docker."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

IMAGE = os.environ.get(
    "SEMANTICA_EXPLORER_IMAGE", "semantica-knowledge-explorer:latest"
)
API_KEY = "semantica-persistence-acceptance-key"


def docker(*arguments: str, capture: bool = False) -> str:
    result = subprocess.run(
        ["docker", *arguments],
        check=True,
        text=True,
        capture_output=capture,
    )
    return result.stdout.strip() if capture else ""


def request(
    url: str,
    *,
    api_key: str | None = None,
    body: bytes | None = None,
    content_type: str | None = None,
    method: str | None = None,
) -> tuple[int, dict]:
    headers = {}
    if api_key:
        headers["X-API-Key"] = api_key
    if content_type:
        headers["Content-Type"] = content_type
    outbound = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(outbound, timeout=5) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def wait_for_health(base_url: str, expected: int, timeout: int = 45) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status, _ = request(f"{base_url}/api/health")
            if status == expected:
                return
        except (OSError, TimeoutError, ValueError):
            pass
        time.sleep(1)
    raise AssertionError(f"Explorer health did not reach HTTP {expected}")


def multipart_graph(node_id: str) -> tuple[bytes, str]:
    boundary = "semantica-persistence-boundary"
    graph = json.dumps({"nodes": [{"id": node_id, "type": "entity"}], "edges": []})
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="graph.json"\r\n'
        "Content-Type: application/json\r\n\r\n"
        f"{graph}\r\n--{boundary}--\r\n"
    ).encode()
    return body, f"multipart/form-data; boundary={boundary}"


def post_json(url: str, payload: dict) -> tuple[int, dict]:
    return request(
        url,
        api_key=API_KEY,
        body=json.dumps(payload).encode(),
        content_type="application/json",
        method="POST",
    )


def start_explorer(name: str, network: str, namespace: str) -> str:
    docker(
        "run",
        "-d",
        "--name",
        name,
        "--network",
        network,
        "-p",
        "127.0.0.1::8000",
        "-e",
        "FALKORDB_HOST=falkordb",
        "-e",
        "FALKORDB_PORT=6379",
        "-e",
        "FALKORDB_GRAPH_NAME=semantica_persistence_acceptance",
        "-e",
        f"SEMANTICA_GRAPH_NAMESPACE={namespace}",
        "-e",
        "SEMANTICA_PERSISTENCE_REQUIRED=true",
        "-e",
        f"SEMANTICA_API_KEY={API_KEY}",
        IMAGE,
    )
    port = docker("port", name, "8000/tcp", capture=True).rsplit(":", 1)[1]
    url = f"http://127.0.0.1:{port}"
    wait_for_health(url, 200)
    return url


def main() -> None:
    suffix = uuid.uuid4().hex[:12]
    network = f"semantica-persistence-{suffix}"
    database = f"semantica-persistence-db-{suffix}"
    explorer = f"semantica-persistence-api-{suffix}"
    isolated = f"semantica-persistence-isolated-{suffix}"
    with tempfile.TemporaryDirectory(prefix="semantica-persistence-") as data_root:
        data = Path(data_root, "falkordb")
        data.mkdir(mode=0o700)
        docker("network", "create", network)
        try:
            docker(
                "run",
                "-d",
                "--name",
                database,
                "--network",
                network,
                "--network-alias",
                "falkordb",
                "-v",
                f"{data}:/var/lib/falkordb/data",
                "falkordb/falkordb:latest",
            )
            for cycle in range(2):
                node_id = f"durable-node-{cycle}"
                base_url = start_explorer(explorer, network, "primary")
                body, media_type = multipart_graph(node_id)
                status, _ = request(
                    f"{base_url}/api/import",
                    api_key=API_KEY,
                    body=body,
                    content_type=media_type,
                )
                assert status == 200

                docker("rm", "-f", explorer)
                base_url = start_explorer(explorer, network, "primary")
                status, _ = request(
                    f"{base_url}/api/graph/node/{node_id}", api_key=API_KEY
                )
                assert status == 200

                docker("stop", database)
                wait_for_health(base_url, 503)
                docker("start", database)
                wait_for_health(base_url, 200)
                status, _ = request(
                    f"{base_url}/api/graph/node/{node_id}", api_key=API_KEY
                )
                assert status == 200
                docker("rm", "-f", explorer)

            isolated_url = start_explorer(isolated, network, "isolated")
            status, _ = request(
                f"{isolated_url}/api/graph/node/durable-node-0", api_key=API_KEY
            )
            assert status == 404

            docker("rm", "-f", isolated)
            base_url = start_explorer(explorer, network, "primary")
            boundary = "semantica-lifecycle-boundary"
            lifecycle_graph = json.dumps(
                {
                    "nodes": [
                        {
                            "id": "lifecycle-primary",
                            "type": "knx__memory__tenant-primary",
                            "properties": {
                                "knx_tenant_id": "tenant-primary",
                                "knx_payload": {
                                    "id": "memory-primary",
                                    "tenant_id": "tenant-primary",
                                    "subject_id": "subject-primary",
                                },
                            },
                        },
                        {
                            "id": "lifecycle-foreign",
                            "type": "knx__memory__tenant-primary",
                            "properties": {
                                "knx_tenant_id": "tenant-primary",
                                "knx_payload": {
                                    "id": "memory-foreign",
                                    "tenant_id": "tenant-primary",
                                    "subject_id": "subject-foreign",
                                },
                            },
                        },
                    ],
                    "edges": [],
                }
            )
            lifecycle_body = (
                f"--{boundary}\r\n"
                'Content-Disposition: form-data; name="file"; '
                'filename="lifecycle.json"\r\n'
                "Content-Type: application/json\r\n\r\n"
                f"{lifecycle_graph}\r\n--{boundary}--\r\n"
            ).encode()
            status, _ = request(
                f"{base_url}/api/import",
                api_key=API_KEY,
                body=lifecycle_body,
                content_type=f"multipart/form-data; boundary={boundary}",
            )
            assert status == 200
            status, evidence = post_json(
                f"{base_url}/api/lifecycle/subjects/purge",
                {
                    "tenant_id": "tenant-primary",
                    "subject_id": "subject-primary",
                    "reason": "automated acceptance",
                },
            )
            assert status == 200 and evidence["status"] == "complete"
            status, _ = request(
                f"{base_url}/api/graph/node/lifecycle-foreign", api_key=API_KEY
            )
            assert status == 200
            docker("rm", "-f", explorer)
            base_url = start_explorer(explorer, network, "primary")
            status, verification = post_json(
                f"{base_url}/api/lifecycle/subjects/verify",
                {
                    "tenant_id": "tenant-primary",
                    "subject_id": "subject-primary",
                    "node_ids": evidence["purged_node_ids"],
                    "artifact_ids": evidence["artifact_ids"],
                },
            )
            assert status == 200 and verification["status"] == "complete"
            print(
                "Explorer persistence acceptance passed twice; "
                "lifecycle restart acceptance passed"
            )
        finally:
            subprocess.run(
                ["docker", "rm", "-f", explorer, isolated, database],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                ["docker", "network", "rm", network],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


if __name__ == "__main__":
    main()
