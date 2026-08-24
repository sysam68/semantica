"""FalkorDB adapter for durable Explorer graph snapshots."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Protocol


class SnapshotPersistenceUnavailable(RuntimeError):
    """Raised without leaking backend connection or credential details."""


class GraphStoreClient(Protocol):
    def connect(self) -> bool: ...

    def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        **options: Any,
    ) -> Dict[str, Any]: ...

    def close(self) -> None: ...


_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class FalkorDBGraphSnapshotRepository:
    """Store one authoritative serialized ContextGraph per namespace."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        graph_name: str,
        namespace: str,
        password: Optional[str] = None,
        store_factory: Optional[Callable[..., GraphStoreClient]] = None,
    ) -> None:
        if not host.strip():
            raise ValueError("FalkorDB host is required")
        if port < 1 or port > 65535:
            raise ValueError("FalkorDB port must be between 1 and 65535")
        for name, value in (("graph name", graph_name), ("namespace", namespace)):
            if not _SAFE_NAME.fullmatch(value):
                raise ValueError(f"FalkorDB {name} is invalid")
        if store_factory is None:
            from ..graph_store import GraphStore

            store_factory = GraphStore
        self._namespace = namespace
        self._store = store_factory(
            backend="falkordb",
            host=host,
            port=port,
            password=password,
            graph_name=graph_name,
        )
        try:
            if not self._store.connect():
                raise SnapshotPersistenceUnavailable(
                    "durable graph storage is unavailable"
                )
            if not self.health():
                raise SnapshotPersistenceUnavailable(
                    "durable graph storage is unavailable"
                )
        except Exception as exc:
            raise SnapshotPersistenceUnavailable(
                "durable graph storage is unavailable"
            ) from exc

    def load(self) -> Optional[Dict[str, Any]]:
        try:
            result = self._store.execute_query(
                "MATCH (s:SemanticaExplorerSnapshot {namespace: $namespace}) "
                "RETURN s.payload LIMIT 1",
                {"namespace": self._namespace},
            )
            records = result.get("records", [])
            if not records:
                return None
            raw = records[0][0]
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            if not isinstance(raw, str):
                raise ValueError("snapshot payload is not text")
            snapshot = json.loads(raw)
            if not isinstance(snapshot, dict):
                raise ValueError("snapshot payload is not an object")
            return snapshot
        except Exception as exc:
            raise SnapshotPersistenceUnavailable(
                "durable graph snapshot cannot be loaded"
            ) from exc

    def save(self, snapshot: Dict[str, Any]) -> None:
        try:
            payload = json.dumps(
                snapshot,
                default=_json_value,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            result = self._store.execute_query(
                "MERGE (s:SemanticaExplorerSnapshot {namespace: $namespace}) "
                "SET s.payload = $payload, s.updated_at = $updated_at "
                "RETURN s.namespace",
                {
                    "namespace": self._namespace,
                    "payload": payload,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            if not result.get("success"):
                raise RuntimeError("snapshot write was not acknowledged")
        except Exception as exc:
            raise SnapshotPersistenceUnavailable(
                "durable graph snapshot cannot be saved"
            ) from exc

    def health(self) -> bool:
        try:
            result = self._store.execute_query("RETURN 1", {})
            return bool(result.get("success"))
        except Exception:
            return False

    def close(self) -> None:
        self._store.close()


def _json_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"unsupported snapshot value: {type(value).__name__}")
