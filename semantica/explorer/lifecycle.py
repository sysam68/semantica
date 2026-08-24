"""Backend-neutral private lifecycle operations for the Explorer data plane."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import Any, Dict, List, Optional

from .session import GraphSession

MAX_LIFECYCLE_NODES = 5000
PUBLICATION_DATA_KIND = "publications"
_REFERENCE_KEYS = frozenset(
    {
        "target_id",
        "replacement_id",
        "source_id",
        "memory_id",
        "observation_id",
        "inference_id",
        "provenance_ids",
        "statement_ids",
        "used_entities",
    }
)


def _properties(node: Mapping[str, Any]) -> Mapping[str, Any]:
    value = node.get("properties")
    return value if isinstance(value, Mapping) else {}


def _payload(node: Mapping[str, Any]) -> Mapping[str, Any]:
    value = _properties(node).get("knx_payload")
    return value if isinstance(value, Mapping) else {}


def _scope(node: Mapping[str, Any]) -> tuple[Optional[str], Optional[str]]:
    properties = _properties(node)
    payload = _payload(node)
    tenant = properties.get("knx_tenant_id", payload.get("tenant_id"))
    subject = properties.get("knx_subject_id", payload.get("subject_id"))
    return (
        str(tenant) if tenant is not None else None,
        str(subject) if subject is not None else None,
    )


def _artifact_id(node: Mapping[str, Any]) -> Optional[str]:
    value = _payload(node).get("id")
    return str(value) if value is not None else None


def _data_kind(node: Mapping[str, Any]) -> Optional[str]:
    value = _properties(node).get("knx_data_kind")
    return str(value) if value is not None else None


def _payload_digest(node: Mapping[str, Any]) -> str:
    encoded = json.dumps(_payload(node), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _references(node: Mapping[str, Any]) -> set[str]:
    values: set[str] = set()
    payload = _payload(node)
    properties = _properties(node)
    for source in (properties, payload):
        for key in _REFERENCE_KEYS:
            value = source.get(key)
            if isinstance(value, (list, tuple, set)):
                values.update(str(item) for item in value if item is not None)
            elif value is not None:
                values.add(str(value))
    knx_target = properties.get("knx_target_id")
    if knx_target is not None:
        values.add(str(knx_target))
    return values


def _snapshot_nodes(snapshot: Optional[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    if snapshot is None:
        return []
    nodes = snapshot.get("nodes")
    if not isinstance(nodes, list):
        return []
    return [dict(node) for node in nodes if isinstance(node, Mapping)]


class LifecycleService:
    def __init__(self, session: GraphSession, *, limit: int = MAX_LIFECYCLE_NODES):
        self._session = session
        self._limit = limit

    def scoped_nodes(
        self,
        tenant_id: str,
        subject_id: str,
        *,
        kinds: Iterable[str] = (),
    ) -> List[Dict[str, Any]]:
        nodes = self._session.lifecycle_nodes(limit=self._limit)
        return self._select_scoped_nodes(
            nodes,
            tenant_id,
            subject_id,
            kinds=frozenset(str(kind) for kind in kinds),
        )

    def _select_scoped_nodes(
        self,
        nodes: List[Dict[str, Any]],
        tenant_id: str,
        subject_id: str,
        *,
        kinds: frozenset[str],
    ) -> List[Dict[str, Any]]:
        selected: Dict[str, Dict[str, Any]] = {}
        for node in nodes:
            node_tenant, node_subject = _scope(node)
            data_kind = _data_kind(node)
            kind_matches = not kinds or data_kind in kinds
            exact_scope = (node_tenant, node_subject) == (tenant_id, subject_id)
            subject_owned_publication = (
                PUBLICATION_DATA_KIND in kinds
                and data_kind == PUBLICATION_DATA_KIND
                and node_subject == subject_id
            )
            if kind_matches and (exact_scope or subject_owned_publication):
                selected[str(node["id"])] = node
        artifact_ids = {
            artifact_id
            for node in selected.values()
            if (artifact_id := _artifact_id(node)) is not None
        }
        payload_digests = {_payload_digest(node) for node in selected.values()}
        changed = True
        while changed:
            changed = False
            for node in nodes:
                node_id = str(node["id"])
                node_tenant, _ = _scope(node)
                if node_id in selected or node_tenant != tenant_id:
                    continue
                stored_digest = _properties(node).get("knx_payload_digest")
                references_selected = not _references(node).isdisjoint(artifact_ids)
                matches_idempotency = (
                    stored_digest is not None and str(stored_digest) in payload_digests
                )
                if not references_selected and not matches_idempotency:
                    continue
                selected[node_id] = node
                artifact_id = _artifact_id(node)
                if artifact_id is not None:
                    artifact_ids.add(artifact_id)
                payload_digests.add(_payload_digest(node))
                changed = True
        if len(selected) > self._limit:
            raise ValueError("lifecycle node limit exceeded")
        return [selected[node_id] for node_id in sorted(selected)]

    def purge_subject(
        self,
        tenant_id: str,
        subject_id: str,
        *,
        reason: str,
        kinds: Iterable[str] = (),
    ) -> tuple[List[str], List[str], Dict[str, Dict[str, Any]]]:
        requested_kinds = tuple(str(kind) for kind in kinds)
        nodes = self.scoped_nodes(
            tenant_id,
            subject_id,
            kinds=requested_kinds,
        )
        node_ids = [str(node["id"]) for node in nodes]
        artifact_ids = sorted(
            artifact_id
            for node in nodes
            if (artifact_id := _artifact_id(node)) is not None
        )
        purged = self._session.purge_nodes_atomically(
            node_ids,
            reason=reason,
            cascade=True,
        )
        checks = self.verify(
            tenant_id,
            subject_id,
            node_ids=purged,
            artifact_ids=artifact_ids,
            kinds=requested_kinds,
        )
        return purged, artifact_ids, checks

    def purge_node(
        self,
        node_id: str,
        tenant_id: str,
        subject_id: str,
        *,
        reason: str,
        cascade: bool,
    ) -> tuple[List[str], List[str], Dict[str, Dict[str, Any]]]:
        node = self._session.get_node(node_id)
        if node is None:
            raise KeyError(node_id)
        if _scope(node) != (tenant_id, subject_id):
            raise KeyError(node_id)
        artifact_id = _artifact_id(node)
        purged = self._session.purge_nodes_atomically(
            [node_id], reason=reason, cascade=cascade
        )
        artifact_ids = [artifact_id] if artifact_id is not None else []
        return (
            purged,
            artifact_ids,
            self.verify(
                tenant_id,
                subject_id,
                node_ids=purged or [node_id],
                artifact_ids=artifact_ids,
            ),
        )

    def verify(
        self,
        tenant_id: str,
        subject_id: str,
        *,
        node_ids: Iterable[str],
        artifact_ids: Iterable[str],
        kinds: Iterable[str] = (),
    ) -> Dict[str, Dict[str, Any]]:
        target_nodes = {str(value) for value in node_ids}
        target_artifacts = {str(value) for value in artifact_ids}
        nodes = self._session.lifecycle_nodes(limit=self._limit)
        live_ids = {str(node["id"]) for node in nodes}
        requested_kinds = tuple(str(kind) for kind in kinds)
        scoped_ids = {
            str(node["id"])
            for node in self.scoped_nodes(
                tenant_id,
                subject_id,
                kinds=requested_kinds,
            )
        }
        edges = list(self._session.graph.edges)

        checks: Dict[str, Dict[str, Any]] = {}
        checks["direct_lookup"] = self._check(live_ids & target_nodes)
        checks["enumeration"] = self._check(scoped_ids)
        checks["graph_traversal"] = self._check(
            edge.edge_id
            for edge in edges
            if edge.source_id in target_nodes or edge.target_id in target_nodes
        )
        checks["semantic_recall"] = self._check(
            node_id
            for node_id in target_nodes
            if self._session.search_index_contains(node_id)
        )
        embeddings = self._session.get_cached_embeddings(force_refresh=True)
        checks["vector_similarity"] = self._check(target_nodes & set(embeddings))
        checks["cached_retrieval"] = checks["semantic_recall"]
        checks["temporal_lookup"] = self._check(
            str(node.get("id"))
            for node in self._session.graph.state_at("9999-12-31T23:59:59+00:00").get(
                "nodes", []
            )
            if str(node.get("id")) in target_nodes
        )
        export_ids = {
            str(node.get("id"))
            for node in self._session.build_graph_dict().get("entities", [])
        }
        checks["export"] = self._check(export_ids & target_nodes)
        checks["orphan_relationships"] = self._check(
            edge.edge_id
            for edge in edges
            if edge.source_id not in live_ids or edge.target_id not in live_ids
        )
        checks["unreferenced_vectors"] = self._check(set(embeddings) - live_ids)
        checks["provenance"] = self._verify_provenance(target_nodes | target_artifacts)
        checks["restart_recovery"] = self._verify_snapshot(
            tenant_id,
            subject_id,
            target_nodes,
            requested_kinds,
        )
        return checks

    def _verify_provenance(self, target_ids: set[str]) -> Dict[str, Any]:
        manager = self._session._provenance_manager
        if manager is None:
            return self._not_configured()
        entries = manager.storage.retrieve_all()
        if len(entries) > self._limit:
            return self._check(["verification-limit-exceeded"])
        residuals = []
        for entry in entries[: self._limit + 1]:
            referenced = {
                str(value)
                for value in (
                    entry.entity_id,
                    entry.parent_entity_id,
                    entry.previous_version_id,
                    entry.derived_from_id,
                    *(entry.used_entities or []),
                )
                if value is not None
            }
            if not referenced.isdisjoint(target_ids):
                residuals.append(str(entry.entity_id))
        return self._check(residuals)

    def _verify_snapshot(
        self,
        tenant_id: str,
        subject_id: str,
        target_nodes: set[str],
        kinds: tuple[str, ...],
    ) -> Dict[str, Any]:
        snapshot = self._session.load_persisted_graph()
        if snapshot is None:
            return self._not_configured()
        snapshot_nodes = _snapshot_nodes(snapshot)
        if len(snapshot_nodes) > self._limit:
            return self._check(["verification-limit-exceeded"])
        scoped_snapshot_ids = {
            str(node["id"])
            for node in self._select_scoped_nodes(
                snapshot_nodes,
                tenant_id,
                subject_id,
                kinds=frozenset(kinds),
            )
        }
        snapshot_ids = {str(node.get("id", "")) for node in snapshot_nodes}
        residuals = sorted(scoped_snapshot_ids | (target_nodes & snapshot_ids))
        return self._check(residuals)

    @staticmethod
    def _check(values: Iterable[str]) -> Dict[str, Any]:
        residuals = sorted({str(value) for value in values if value})
        return {
            "status": "residual" if residuals else "absent",
            "residual_count": len(residuals),
            "sample_ids": residuals[:20],
        }

    @staticmethod
    def _not_configured() -> Dict[str, Any]:
        return {"status": "not_configured", "residual_count": 0, "sample_ids": []}
