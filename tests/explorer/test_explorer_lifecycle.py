"""Contracts for authenticated lifecycle purge and absence verification."""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Dict, Optional

import pytest

from semantica.context.context_graph import ContextGraph
from semantica.explorer.app import create_app
from semantica.explorer.falkordb_snapshot import SnapshotPersistenceUnavailable
from semantica.explorer.session import GraphSession

try:
    from starlette.testclient import TestClient
except ImportError:
    pytest.skip(
        "starlette TestClient is required for explorer tests. "
        "Install semantica[explorer].",
        allow_module_level=True,
    )


class MemorySnapshotRepository:
    def __init__(self, state: Dict[str, Any]) -> None:
        self.state = state
        self.available = True

    def load(self) -> Optional[Dict[str, Any]]:
        value = self.state.get("snapshot")
        return copy.deepcopy(value) if value is not None else None

    def save(self, snapshot: Dict[str, Any]) -> None:
        if not self.available:
            raise SnapshotPersistenceUnavailable("unavailable")
        self.state["snapshot"] = copy.deepcopy(snapshot)

    def health(self) -> bool:
        return self.available

    def close(self) -> None:
        return None


def node(
    node_id: str,
    *,
    tenant: str,
    artifact_id: str,
    subject: Optional[str] = None,
    target: Optional[str] = None,
    embedding: Optional[list[float]] = None,
    data_kind: str = "observations",
) -> dict:
    payload: Dict[str, Any] = {
        "id": artifact_id,
        "tenant_id": tenant,
    }
    if subject is not None:
        payload["subject_id"] = subject
    if target is not None:
        payload["target_id"] = target
    properties: Dict[str, Any] = {
        "knx_tenant_id": tenant,
        "knx_data_kind": data_kind,
        "knx_payload": payload,
    }
    if target is not None:
        properties["knx_target_id"] = target
    if embedding is not None:
        properties["embedding"] = embedding
    return {
        "id": node_id,
        "type": f"knx__observation__{tenant}",
        "properties": properties,
    }


def import_nodes(client: TestClient, nodes: list[dict]) -> None:
    response = client.post(
        "/api/import",
        headers={"X-API-Key": "test-api-key"},
        files={
            "file": (
                "knx.json",
                json.dumps({"nodes": nodes, "edges": []}),
                "application/json",
            )
        },
    )
    assert response.status_code == 200, response.text


def test_subject_purge_is_scoped_idempotent_and_survives_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SEMANTICA_API_KEY", "test-api-key")
    shared: Dict[str, Any] = {}
    repository = MemorySnapshotRepository(shared)
    subject_nodes = [
        node(
            "knx-observation-one",
            tenant="tenant-one",
            artifact_id="observation-one",
            subject="subject-one",
        ),
        node(
            "knx-memory-one",
            tenant="tenant-one",
            artifact_id="memory-one",
            subject="subject-one",
            embedding=[0.1, 0.2],
        ),
        node(
            "knx-validation-one",
            tenant="tenant-one",
            artifact_id="validation-one",
            target="observation-one",
        ),
    ]
    observation_digest = hashlib.sha256(
        json.dumps(
            subject_nodes[0]["properties"]["knx_payload"],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    subject_nodes.append(
        {
            "id": "knx-idempotency-one",
            "type": "knx__idempotency__tenant-one",
            "properties": {
                "knx_tenant_id": "tenant-one",
                "knx_kind": "idempotency",
                "knx_payload": {},
                "knx_payload_digest": observation_digest,
            },
        }
    )
    foreign_nodes = [
        node(
            "foreign-subject",
            tenant="tenant-one",
            artifact_id="foreign-subject",
            subject="subject-two",
        ),
        node(
            "foreign-tenant",
            tenant="tenant-two",
            artifact_id="foreign-tenant",
            subject="subject-one",
        ),
    ]

    with TestClient(create_app(snapshot_repository=repository)) as client:
        import_nodes(client, subject_nodes + foreign_nodes)
        enumerated = client.post(
            "/api/lifecycle/subjects/enumerate",
            headers={"X-API-Key": "test-api-key"},
            json={"tenant_id": "tenant-one", "subject_id": "subject-one"},
        )
        purged = client.post(
            "/api/lifecycle/subjects/purge",
            headers={"X-API-Key": "test-api-key"},
            json={
                "tenant_id": "tenant-one",
                "subject_id": "subject-one",
                "reason": "authorized erasure request",
            },
        )
        replay = client.post(
            "/api/lifecycle/subjects/purge",
            headers={"X-API-Key": "test-api-key"},
            json={
                "tenant_id": "tenant-one",
                "subject_id": "subject-one",
                "reason": "authorized erasure request",
            },
        )
        foreign_subject = client.get(
            "/api/graph/node/foreign-subject",
            headers={"X-API-Key": "test-api-key"},
        )
        foreign_tenant = client.get(
            "/api/graph/node/foreign-tenant",
            headers={"X-API-Key": "test-api-key"},
        )

    assert enumerated.status_code == 200
    assert set(enumerated.json()["node_ids"]) == {
        "knx-observation-one",
        "knx-memory-one",
        "knx-validation-one",
        "knx-idempotency-one",
    }
    assert purged.status_code == 200
    evidence = purged.json()
    assert evidence["status"] == "complete"
    assert evidence["purged_count"] == 4
    assert all(check["status"] != "residual" for check in evidence["checks"].values())
    assert replay.status_code == 200
    assert replay.json()["purged_count"] == 0
    assert foreign_subject.status_code == 200
    assert foreign_tenant.status_code == 200

    with TestClient(
        create_app(snapshot_repository=MemorySnapshotRepository(shared))
    ) as restarted:
        verified = restarted.post(
            "/api/lifecycle/subjects/verify",
            headers={"X-API-Key": "test-api-key"},
            json={
                "tenant_id": "tenant-one",
                "subject_id": "subject-one",
                "node_ids": evidence["purged_node_ids"],
                "artifact_ids": evidence["artifact_ids"],
            },
        )

    assert verified.status_code == 200
    assert verified.json()["status"] == "complete"
    assert verified.json()["checks"]["restart_recovery"]["status"] == "absent"


def test_purge_failure_rolls_back_memory_and_returns_sanitized_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SEMANTICA_API_KEY", "test-api-key")
    repository = MemorySnapshotRepository({})
    with TestClient(create_app(snapshot_repository=repository)) as client:
        import_nodes(
            client,
            [
                node(
                    "protected-node",
                    tenant="tenant-one",
                    artifact_id="protected",
                    subject="subject-one",
                )
            ],
        )
        repository.available = False
        failed = client.post(
            "/api/lifecycle/subjects/purge",
            headers={"X-API-Key": "test-api-key"},
            json={
                "tenant_id": "tenant-one",
                "subject_id": "subject-one",
                "reason": "authorized erasure request",
            },
        )
        repository.available = True
        retained = client.get(
            "/api/graph/node/protected-node",
            headers={"X-API-Key": "test-api-key"},
        )

    assert failed.status_code == 503
    assert "falkordb" not in failed.text.lower()
    assert retained.status_code == 200


def test_scoped_kind_purge_retains_other_subject_data_and_verifies_after_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SEMANTICA_API_KEY", "test-api-key")
    shared: Dict[str, Any] = {}
    with TestClient(
        create_app(snapshot_repository=MemorySnapshotRepository(shared))
    ) as client:
        import_nodes(
            client,
            [
                node(
                    "scoped-observation",
                    tenant="tenant-one",
                    artifact_id="scoped-observation",
                    subject="subject-one",
                    data_kind="observations",
                ),
                node(
                    "retained-memory",
                    tenant="tenant-one",
                    artifact_id="retained-memory",
                    subject="subject-one",
                    data_kind="memories",
                ),
            ],
        )
        purged = client.post(
            "/api/lifecycle/subjects/purge",
            headers={"X-API-Key": "test-api-key"},
            json={
                "tenant_id": "tenant-one",
                "subject_id": "subject-one",
                "kinds": ["observations"],
                "reason": "authorized scoped forget",
            },
        )
        retained = client.get(
            "/api/graph/node/retained-memory",
            headers={"X-API-Key": "test-api-key"},
        )

    assert purged.status_code == 200
    evidence = purged.json()
    assert evidence["status"] == "complete"
    assert evidence["purged_node_ids"] == ["scoped-observation"]
    assert retained.status_code == 200

    with TestClient(
        create_app(snapshot_repository=MemorySnapshotRepository(shared))
    ) as restarted:
        verified = restarted.post(
            "/api/lifecycle/subjects/verify",
            headers={"X-API-Key": "test-api-key"},
            json={
                "tenant_id": "tenant-one",
                "subject_id": "subject-one",
                "kinds": ["observations"],
                "node_ids": evidence["purged_node_ids"],
                "artifact_ids": evidence["artifact_ids"],
            },
        )
        memories = restarted.post(
            "/api/lifecycle/subjects/enumerate",
            headers={"X-API-Key": "test-api-key"},
            json={
                "tenant_id": "tenant-one",
                "subject_id": "subject-one",
                "kinds": ["memories"],
            },
        )

    assert verified.status_code == 200
    assert verified.json()["status"] == "complete"
    assert memories.json()["node_ids"] == ["retained-memory"]


def test_purge_reports_provenance_residual_without_subject_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SEMANTICA_API_KEY", "test-api-key")
    session = GraphSession(
        ContextGraph(advanced_analytics=False),
        snapshot_repository=MemorySnapshotRepository({}),
    )
    app = create_app(session=session)
    with TestClient(app) as client:
        import_nodes(
            client,
            [
                node(
                    "provenance-node",
                    tenant="tenant-one",
                    artifact_id="provenance-artifact",
                    subject="subject-one",
                )
            ],
        )
        session.provenance_manager.track_entity(
            "provenance-node",
            source="subject content must not be returned",
        )
        purged = client.post(
            "/api/lifecycle/subjects/purge",
            headers={"X-API-Key": "test-api-key"},
            json={
                "tenant_id": "tenant-one",
                "subject_id": "subject-one",
                "reason": "authorized erasure request",
            },
        )

    assert purged.status_code == 200
    assert purged.json()["status"] == "residual"
    provenance = purged.json()["checks"]["provenance"]
    assert provenance["status"] == "residual"
    assert provenance["sample_ids"] == ["provenance-node"]
    assert "subject content" not in purged.text


def test_node_purge_fails_closed_for_cross_scope_and_missing_authentication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SEMANTICA_API_KEY", "test-api-key")
    monkeypatch.delenv("SEMANTICA_ALLOW_ANONYMOUS", raising=False)
    with TestClient(create_app()) as client:
        import_nodes(
            client,
            [
                node(
                    "foreign-node",
                    tenant="tenant-two",
                    artifact_id="foreign",
                    subject="subject-two",
                )
            ],
        )
        unauthorized = client.request(
            "DELETE",
            "/api/lifecycle/nodes/foreign-node",
            json={
                "tenant_id": "tenant-two",
                "subject_id": "subject-two",
                "reason": "authorized erasure request",
                "cascade": True,
            },
        )
        cross_scope = client.request(
            "DELETE",
            "/api/lifecycle/nodes/foreign-node",
            headers={"X-API-Key": "test-api-key"},
            json={
                "tenant_id": "tenant-one",
                "subject_id": "subject-one",
                "reason": "authorized erasure request",
                "cascade": True,
            },
        )
        stale = client.request(
            "DELETE",
            "/api/lifecycle/nodes/missing-node",
            headers={"X-API-Key": "test-api-key"},
            json={
                "tenant_id": "tenant-two",
                "subject_id": "subject-two",
                "reason": "authorized erasure request",
                "cascade": True,
            },
        )

    assert unauthorized.status_code == 401
    assert cross_scope.status_code == 404
    assert stale.status_code == 404
