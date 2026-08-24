"""Contract tests for the durable Explorer graph boundary."""

from __future__ import annotations

import copy
from typing import Any, Dict, Optional

import pytest

from semantica.explorer.app import create_app
from semantica.explorer.falkordb_snapshot import (
    FalkorDBGraphSnapshotRepository,
    SnapshotPersistenceUnavailable,
)

try:
    from starlette.testclient import TestClient
except ImportError:
    pytest.skip(
        "starlette TestClient is required for explorer tests. "
        "Install semantica[explorer].",
        allow_module_level=True,
    )


class MemorySnapshotRepository:
    def __init__(self, state: Dict[str, Dict[str, Any]], namespace: str) -> None:
        self._state = state
        self._namespace = namespace
        self.available = True
        self.closed = False

    def load(self) -> Optional[Dict[str, Any]]:
        snapshot = self._state.get(self._namespace)
        return copy.deepcopy(snapshot) if snapshot is not None else None

    def save(self, snapshot: Dict[str, Any]) -> None:
        if not self.available:
            raise SnapshotPersistenceUnavailable("unavailable")
        self._state[self._namespace] = copy.deepcopy(snapshot)

    def health(self) -> bool:
        return self.available

    def close(self) -> None:
        self.closed = True


class FakeGraphStore:
    def __init__(self, snapshots: Dict[str, str], **config: Any) -> None:
        self.snapshots = snapshots
        self.config = config
        self.connected = False
        self.closed = False

    def connect(self) -> bool:
        self.connected = True
        return True

    def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        **_options: Any,
    ) -> Dict[str, Any]:
        parameters = parameters or {}
        if query == "RETURN 1":
            return {"success": True, "records": [[1]]}
        namespace = parameters["namespace"]
        if query.startswith("MATCH"):
            payload = self.snapshots.get(namespace)
            return {"success": True, "records": [] if payload is None else [[payload]]}
        self.snapshots[namespace] = parameters["payload"]
        return {"success": True, "records": [[namespace]]}

    def close(self) -> None:
        self.closed = True


def test_falkordb_repository_round_trips_isolated_namespaces() -> None:
    snapshots: Dict[str, str] = {}

    def factory(**config: Any) -> FakeGraphStore:
        return FakeGraphStore(snapshots, **config)

    first = FalkorDBGraphSnapshotRepository(
        host="falkordb",
        port=6379,
        graph_name="semantica_explorer",
        namespace="run-one",
        store_factory=factory,
    )
    second = FalkorDBGraphSnapshotRepository(
        host="falkordb",
        port=6379,
        graph_name="semantica_explorer",
        namespace="run-two",
        store_factory=factory,
    )

    first.save({"nodes": [{"id": "one", "type": "entity"}], "edges": []})

    restored = first.load()
    assert restored is not None
    assert restored["nodes"][0]["id"] == "one"
    assert second.load() is None


def test_imported_graph_survives_application_reconstruction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SEMANTICA_API_KEY", "test-api-key")
    shared: Dict[str, Dict[str, Any]] = {}
    first_repository = MemorySnapshotRepository(shared, "evaluation")
    with TestClient(create_app(snapshot_repository=first_repository)) as client:
        imported = client.post(
            "/api/import",
            headers={"X-API-Key": "test-api-key"},
            files={
                "file": (
                    "graph.json",
                    '{"nodes":[{"id":"persisted","type":"entity","content":"safe"}],"edges":[]}',
                    "application/json",
                )
            },
        )
        assert imported.status_code == 200
    assert first_repository.closed

    second_repository = MemorySnapshotRepository(shared, "evaluation")
    with TestClient(create_app(snapshot_repository=second_repository)) as client:
        restored = client.get(
            "/api/graph/node/persisted",
            headers={"X-API-Key": "test-api-key"},
        )

    assert restored.status_code == 200
    assert restored.json()["id"] == "persisted"


def test_persistence_outage_fails_health_and_mutation_without_backend_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SEMANTICA_API_KEY", "test-api-key")
    repository = MemorySnapshotRepository({}, "evaluation")
    with TestClient(create_app(snapshot_repository=repository)) as client:
        repository.available = False
        health = client.get("/api/health")
        imported = client.post(
            "/api/import",
            headers={"X-API-Key": "test-api-key"},
            files={
                "file": (
                    "graph.json",
                    '{"nodes":[{"id":"not-durable","type":"entity"}],"edges":[]}',
                    "application/json",
                )
            },
        )
        repository.available = True
        recovered = client.get("/api/health")

    assert health.status_code == 503
    assert health.headers["content-type"].startswith("application/problem+json")
    assert health.json()["detail"] == "The durable graph dependency is unavailable."
    assert imported.status_code == 503
    assert "falkordb" not in imported.text.lower()
    assert recovered.status_code == 200


def test_repository_fails_closed_when_storage_is_unhealthy() -> None:
    class UnhealthyGraphStore(FakeGraphStore):
        def execute_query(
            self,
            query: str,
            parameters: Optional[Dict[str, Any]] = None,
            **options: Any,
        ) -> Dict[str, Any]:
            if query == "RETURN 1":
                return {"success": False, "records": []}
            return super().execute_query(query, parameters, **options)

    with pytest.raises(SnapshotPersistenceUnavailable):
        FalkorDBGraphSnapshotRepository(
            host="falkordb",
            port=6379,
            graph_name="semantica_explorer",
            namespace="evaluation",
            store_factory=lambda **config: UnhealthyGraphStore({}, **config),
        )


def test_openapi_documents_durable_dependency_failures() -> None:
    with TestClient(create_app()) as client:
        schema = client.get("/openapi.json").json()

    for path in ("/api/health", "/api/import"):
        operation = "get" if path == "/api/health" else "post"
        unavailable = schema["paths"][path][operation]["responses"]["503"]
        assert "application/problem+json" in unavailable["content"]
