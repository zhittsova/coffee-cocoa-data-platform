"""Reject unmanaged dbt warehouse connections before DuckDB opens the file."""

import os
from pathlib import Path

from dbt.adapters.duckdb.plugins import BasePlugin


class Plugin(BasePlugin):
    def update_connection_config(self, creds, config):
        if creds.path == ":memory:":
            return
        try:
            fd = int(os.environ["COFFEE_COCOA_LOCK_FD"])
            expected = Path(os.environ["COFFEE_COCOA_LOCK_PATH"])
            actual, wanted = os.fstat(fd), expected.stat()
            if (actual.st_dev, actual.st_ino) != (wanted.st_dev, wanted.st_ino):
                raise ValueError("wrong lock inode")
            root = expected.parent.parent.resolve()
            database = Path(creds.path).resolve()
            if database != root / "warehouse/coffee_cocoa.duckdb":
                raise ValueError("database is outside coordinated root")
        except (KeyError, OSError, ValueError) as exc:
            raise RuntimeError(
                "Use the coordinated dbt_cli or pipeline entry point for this warehouse"
            ) from exc
