"""Default local paths; importing this module does not create files."""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    data: Path
    raw: Path
    parquet: Path
    warehouse: Path
    snapshots: Path
    state: Path

    def require_capture_root(self) -> None:
        """Keep standalone source commands out of replacement-managed roots."""
        if (self.state / "replacements").exists():
            raise ValueError(
                "Use a separate capture root; this root is managed by source replacements"
            )

    @classmethod
    def from_root(cls, root: Path | None = None) -> "ProjectPaths":
        """Resolve paths from an explicit root, COFFEE_COCOA_HOME, or cwd."""
        base = root or Path(os.environ.get("COFFEE_COCOA_HOME", Path.cwd()))
        base = base.expanduser().resolve()
        data = base / "data"
        return cls(
            root=base,
            data=data,
            raw=data / "raw",
            parquet=data / "parquet",
            warehouse=base / "warehouse",
            snapshots=base / "snapshots",
            state=base / ".state",
        )
