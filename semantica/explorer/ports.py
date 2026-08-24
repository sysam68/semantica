"""Private persistence ports for the Explorer application."""

from typing import Any, Dict, Optional, Protocol


class GraphSnapshotRepository(Protocol):
    """Persist the complete active graph behind an isolated namespace."""

    def load(self) -> Optional[Dict[str, Any]]: ...

    def save(self, snapshot: Dict[str, Any]) -> None: ...

    def health(self) -> bool: ...

    def close(self) -> None: ...
