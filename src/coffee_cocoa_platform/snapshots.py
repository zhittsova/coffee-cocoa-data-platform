"""Immutable DuckDB read snapshots with atomic discovery and reader leases."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import tempfile
import threading
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self

import duckdb

from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.runtime import writer_lock

SCHEMA_VERSION = "1"
DEFAULT_RETAIN_INACTIVE = 2
_VERSION_PATTERN = re.compile(r"g[0-9]{20}-[0-9a-f]{12}")
_active_guard = threading.Lock()
_active_readers: dict[Path, int] = {}


class SnapshotError(RuntimeError):
    """A snapshot cannot be published or opened safely."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json(path: Path, document: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _replace_pointer(staged: Path, pointer: Path) -> None:
    os.replace(staged, pointer)


def _atomic_pointer(path: Path, document: dict[str, Any]) -> None:
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=".current-", suffix=".json", delete=False
    ) as handle:
        staged = Path(handle.name)
    try:
        _write_json(staged, document)
        _replace_pointer(staged, path)
        _fsync_directory(path.parent)
    finally:
        staged.unlink(missing_ok=True)


def _safe_layout(paths: ProjectPaths, *, create: bool) -> tuple[Path, Path]:
    base = paths.snapshots
    versions = base / "versions"
    for path in (base, versions):
        if path.is_symlink() or (path.exists() and path.resolve() != path):
            raise SnapshotError("Snapshot directories must use the canonical root")
    if create:
        versions.mkdir(parents=True, exist_ok=True)
    elif not versions.is_dir():
        raise SnapshotError("No published read snapshot is available")
    return base, versions


@contextmanager
def _registry_lock(base: Path, *, exclusive: bool):
    lock = base / "registry.lock"
    if not base.is_dir():
        raise SnapshotError("No published read snapshot is available")
    with lock.open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _pointer(base: Path) -> dict[str, Any]:
    path = base / "current.json"
    try:
        pointer = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise SnapshotError(
            "The current snapshot pointer is missing or invalid"
        ) from exc
    snapshot_id = pointer.get("snapshot_id")
    if (
        pointer.get("schema_version") != SCHEMA_VERSION
        or not isinstance(snapshot_id, str)
        or _VERSION_PATTERN.fullmatch(snapshot_id) is None
        or not isinstance(pointer.get("generation"), int)
        or pointer["generation"] < 1
    ):
        raise SnapshotError("The current snapshot pointer has an invalid schema")
    return pointer


def _manifest(version: Path, expected: dict[str, Any] | None = None) -> dict[str, Any]:
    manifest_path = version / "manifest.json"
    if version.is_symlink() or manifest_path.is_symlink():
        raise SnapshotError("Snapshot versions and manifests must not be symlinks")
    try:
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise SnapshotError("The snapshot manifest is missing or invalid") from exc
    if expected is not None and hashlib.sha256(
        manifest_bytes
    ).hexdigest() != expected.get("manifest_sha256"):
        raise SnapshotError("The snapshot manifest does not match the pointer")
    warehouse = manifest.get("warehouse")
    if (
        manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("snapshot_id") != version.name
        or not isinstance(manifest.get("generation"), int)
        or int(version.name[1:21]) != manifest.get("generation")
        or not isinstance(warehouse, dict)
        or warehouse.get("file") != "coffee_cocoa.duckdb"
        or not isinstance(warehouse.get("bytes"), int)
        or warehouse["bytes"] < 1
        or not isinstance(warehouse.get("sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", warehouse["sha256"]) is None
    ):
        raise SnapshotError("The snapshot manifest has an invalid identity")
    if expected is not None and (
        manifest["snapshot_id"] != expected["snapshot_id"]
        or manifest["generation"] != expected["generation"]
    ):
        raise SnapshotError("The snapshot pointer and manifest disagree")
    return manifest


def _source_manifest(path: Path, dataset: str) -> dict[str, Any] | None:
    if not path.exists():
        return None
    content = path.read_bytes()
    document = json.loads(content)
    summary: dict[str, Any] = {
        "dataset": dataset,
        "manifest_sha256": hashlib.sha256(content).hexdigest(),
        "schema_version": document.get("schema_version"),
        "retrieved_at_utc": document.get("retrieved_at_utc"),
    }
    if dataset == "benchmark_prices":
        summary.update(
            source_capture_ids=[
                document.get("source_capture_id") or document.get("sha256")
            ],
            source_update=document.get("source_update_text"),
            input_start=document.get("input_start"),
            input_end=document.get("input_end"),
        )
    else:
        slices = document.get("slices", [])
        summary.update(
            source_capture_ids=[
                item.get("capture", {}).get("sha256") for item in slices
            ],
            retrieved_at_utc=sorted(
                {
                    item.get("capture", {}).get("retrieved_at_utc")
                    for item in slices
                    if item.get("capture", {}).get("retrieved_at_utc")
                }
            ),
            source_updates=document.get("source_updates"),
            plan_hash=document.get("plan_hash"),
            capture_vintage=document.get("capture_vintage"),
        )
    summary["source_capture_ids"] = sorted(
        capture for capture in summary["source_capture_ids"] if capture
    )
    return summary


def _database_metadata(database: Path, paths: ProjectPaths) -> dict[str, Any]:
    with duckdb.connect(str(database), read_only=True) as connection:
        tables = sorted(row[0] for row in connection.execute("show tables").fetchall())
        revision = None
        capture_ids: list[str] = []
        if "_revision_history" in tables:
            row = connection.execute(
                "select sequence, event from _revision_history order by sequence desc limit 1"
            ).fetchone()
            event = json.loads(row[1])
            revision = {
                "sequence": row[0],
                "run_id": event.get("run_id"),
                "selected_at_utc": event.get("selected_at_utc"),
                "selected_captures": event.get("selected_captures"),
                "scopes": event.get("scopes"),
            }
            captures = json.loads(
                connection.execute("select captures from _revision_state").fetchone()[0]
            )
            capture_ids = sorted(captures)
    source_manifests = [
        summary
        for summary in (
            _source_manifest(
                paths.parquet / "benchmark_prices.json", "benchmark_prices"
            ),
            _source_manifest(paths.parquet / "trade_observations.json", "trade"),
        )
        if summary is not None
    ]
    return {
        "table_names": tables,
        "revision": revision,
        "revision_capture_ids": capture_ids,
        "source_manifests": source_manifests,
    }


def _checkpoint(database: Path) -> None:
    with duckdb.connect(str(database)) as connection:
        connection.execute("checkpoint")
    wal = Path(f"{database}.wal")
    if wal.exists() and wal.stat().st_size:
        raise SnapshotError("The warehouse WAL was not checkpointed")


def _copy_database(source: Path, destination: Path) -> None:
    shutil.copyfile(source, destination)
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())


def _current_generation(base: Path) -> tuple[int, str | None]:
    try:
        pointer = _pointer(base)
    except SnapshotError:
        if (base / "current.json").exists():
            raise
        return 0, None
    return pointer["generation"], pointer["snapshot_id"]


def publish_snapshot(
    root: Path,
    build: dict[str, Any],
    *,
    retain_inactive: int = DEFAULT_RETAIN_INACTIVE,
) -> dict[str, Any]:
    """Publish a complete read snapshot while coordinating with every writer."""
    if (
        not isinstance(build, dict)
        or not isinstance(build.get("kind"), str)
        or not build["kind"]
        or not isinstance(build.get("run_id"), str)
        or not build["run_id"]
    ):
        raise SnapshotError("Snapshot build metadata requires kind and run_id")
    if retain_inactive < 1:
        raise SnapshotError("Retention must preserve at least one previous snapshot")
    paths = ProjectPaths.from_root(root)
    with writer_lock(paths.root):
        base, versions = _safe_layout(paths, create=True)
        warehouse = paths.warehouse / "coffee_cocoa.duckdb"
        if not warehouse.is_file() or warehouse.is_symlink():
            raise SnapshotError("A regular tested warehouse is required")
        _checkpoint(warehouse)
        generation, previous = _current_generation(base)
        generation += 1
        snapshot_id = f"g{generation:020d}-{uuid.uuid4().hex[:12]}"
        staged = versions / f".staging-{snapshot_id}"
        final = versions / snapshot_id
        staged.mkdir()
        try:
            staged_database = staged / "coffee_cocoa.duckdb"
            _copy_database(warehouse, staged_database)
            source_hash = _sha256(warehouse)
            snapshot_hash = _sha256(staged_database)
            if source_hash != snapshot_hash:
                raise SnapshotError("The copied snapshot does not match the warehouse")
            metadata = _database_metadata(staged_database, paths)
            (staged / "reader.lock").touch()
            manifest = {
                "schema_version": SCHEMA_VERSION,
                "snapshot_id": snapshot_id,
                "generation": generation,
                "previous_snapshot_id": previous,
                "published_at_utc": datetime.now(UTC).isoformat(),
                "build": build,
                "warehouse": {
                    "file": "coffee_cocoa.duckdb",
                    "sha256": snapshot_hash,
                    "bytes": staged_database.stat().st_size,
                    "table_names": metadata["table_names"],
                },
                "source_versions": {
                    "revision": metadata["revision"],
                    "revision_capture_ids": metadata["revision_capture_ids"],
                    "manifests": metadata["source_manifests"],
                },
            }
            _write_json(staged / "manifest.json", manifest)
            _fsync_directory(staged)
            manifest_sha256 = _sha256(staged / "manifest.json")
            pointer = {
                "schema_version": SCHEMA_VERSION,
                "snapshot_id": snapshot_id,
                "generation": generation,
                "manifest_sha256": manifest_sha256,
                "published_at_utc": manifest["published_at_utc"],
            }
            retention = {"removed": [], "active": [], "errors": []}
            with _registry_lock(base, exclusive=True):
                os.replace(staged, final)
                _fsync_directory(versions)
                _atomic_pointer(base / "current.json", pointer)
                try:
                    retention = _prune_locked(
                        base, versions, pointer, retain_inactive=retain_inactive
                    )
                except (OSError, SnapshotError) as exc:
                    retention["errors"].append(
                        f"retention failed: {type(exc).__name__}: {exc}"
                    )
        except Exception:
            if staged.exists():
                shutil.rmtree(staged, ignore_errors=True)
            raise
        return {**manifest, "pointer": pointer, "retention": retention}


def _active(path: Path) -> bool:
    with _active_guard:
        return _active_readers.get(path, 0) > 0


def _prune_locked(
    base: Path,
    versions: Path,
    pointer: dict[str, Any],
    *,
    retain_inactive: int,
) -> dict[str, list[str]]:
    keep = {pointer["snapshot_id"]}
    previous = pointer["snapshot_id"]
    for _ in range(retain_inactive):
        document = _manifest(versions / previous)
        previous = document.get("previous_snapshot_id")
        if previous is None:
            break
        if (
            not isinstance(previous, str)
            or _VERSION_PATTERN.fullmatch(previous) is None
        ):
            raise SnapshotError("Snapshot history contains an invalid previous version")
        keep.add(previous)
    result: dict[str, list[str]] = {"removed": [], "active": [], "errors": []}
    for version in sorted(versions.iterdir()):
        if version.name.startswith(".staging-"):
            try:
                shutil.rmtree(version)
                result["removed"].append(version.name)
            except OSError as exc:
                result["errors"].append(f"{version.name}: {exc}")
            continue
        if not version.is_dir() or version.name in keep:
            continue
        if _VERSION_PATTERN.fullmatch(version.name) is None:
            result["errors"].append(f"unexpected entry: {version.name}")
            continue
        lock_path = version / "reader.lock"
        if _active(version):
            result["active"].append(version.name)
            continue
        try:
            with lock_path.open("r+") as lease:
                try:
                    fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    result["active"].append(version.name)
                    continue
                try:
                    shutil.rmtree(version)
                    result["removed"].append(version.name)
                except OSError as exc:
                    result["errors"].append(f"{version.name}: {exc}")
                finally:
                    fcntl.flock(lease, fcntl.LOCK_UN)
        except OSError as exc:
            result["errors"].append(f"{version.name}: {exc}")
    _fsync_directory(versions)
    return result


def prune_snapshots(
    root: Path, *, retain_inactive: int = DEFAULT_RETAIN_INACTIVE
) -> dict[str, list[str]]:
    """Remove unreferenced inactive versions without disturbing leased readers."""
    if retain_inactive < 1:
        raise SnapshotError("Retention must preserve at least one previous snapshot")
    paths = ProjectPaths.from_root(root)
    base, versions = _safe_layout(paths, create=False)
    with _registry_lock(base, exclusive=True):
        pointer = _pointer(base)
        return _prune_locked(base, versions, pointer, retain_inactive=retain_inactive)


class SnapshotReader:
    """A read-only connection pinned to one immutable snapshot version."""

    def __init__(self, root: Path):
        self.root = ProjectPaths.from_root(root).root
        self.connection: duckdb.DuckDBPyConnection | None = None
        self.manifest: dict[str, Any] | None = None
        self.snapshot_id: str | None = None
        self._lease = None
        self._version: Path | None = None
        self.refresh()

    def _open_current(self):
        paths = ProjectPaths.from_root(self.root)
        base, versions = _safe_layout(paths, create=False)
        with _registry_lock(base, exclusive=False):
            pointer = _pointer(base)
            version = versions / pointer["snapshot_id"]
            manifest = _manifest(version, pointer)
            database = version / manifest["warehouse"]["file"]
            lease_path = version / "reader.lock"
            if (
                not database.is_file()
                or database.is_symlink()
                or lease_path.is_symlink()
                or database.parent.resolve() != version
            ):
                raise SnapshotError(
                    "Snapshot files must be regular files in their version"
                )
            lease = lease_path.open("r+")
            try:
                fcntl.flock(lease, fcntl.LOCK_SH)
                with _active_guard:
                    _active_readers[version] = _active_readers.get(version, 0) + 1
                if (
                    database.stat().st_size != manifest["warehouse"]["bytes"]
                    or _sha256(database) != manifest["warehouse"]["sha256"]
                ):
                    raise SnapshotError(
                        "The snapshot database does not match its manifest"
                    )
                connection = duckdb.connect(str(database), read_only=True)
                connection.execute("show tables").fetchall()
            except Exception:
                with _active_guard:
                    count = _active_readers.get(version, 0) - 1
                    if count > 0:
                        _active_readers[version] = count
                    else:
                        _active_readers.pop(version, None)
                fcntl.flock(lease, fcntl.LOCK_UN)
                lease.close()
                raise
        return pointer, manifest, version, lease, connection

    def refresh(self) -> bool:
        """Open the current version before releasing the reader's prior version."""
        pointer, manifest, version, lease, connection = self._open_current()
        if pointer["snapshot_id"] == self.snapshot_id and self.connection is not None:
            connection.close()
            with _active_guard:
                count = _active_readers.get(version, 0) - 1
                if count > 0:
                    _active_readers[version] = count
                else:
                    _active_readers.pop(version, None)
            fcntl.flock(lease, fcntl.LOCK_UN)
            lease.close()
            return False
        old_connection = self.connection
        old_lease = self._lease
        old_version = self._version
        self.connection = connection
        self.manifest = manifest
        self.snapshot_id = pointer["snapshot_id"]
        self._lease = lease
        self._version = version
        if old_connection is not None:
            old_connection.close()
        if old_lease is not None and old_version is not None:
            with _active_guard:
                count = _active_readers.get(old_version, 0) - 1
                if count > 0:
                    _active_readers[old_version] = count
                else:
                    _active_readers.pop(old_version, None)
            fcntl.flock(old_lease, fcntl.LOCK_UN)
            old_lease.close()
        return True

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        if self._lease is not None and self._version is not None:
            with _active_guard:
                count = _active_readers.get(self._version, 0) - 1
                if count > 0:
                    _active_readers[self._version] = count
                else:
                    _active_readers.pop(self._version, None)
            fcntl.flock(self._lease, fcntl.LOCK_UN)
            self._lease.close()
            self._lease = None
            self._version = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args) -> None:
        self.close()


def open_snapshot(root: Path) -> SnapshotReader:
    """Open the atomically selected immutable database, never the warehouse."""
    return SnapshotReader(root)
