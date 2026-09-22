"""One cooperative local writer, including surviving dbt subprocesses."""

import fcntl
import os
import subprocess
import sys
import threading
from contextlib import contextmanager
from pathlib import Path

from dagster import resource

from coffee_cocoa_platform.paths import ProjectPaths

_local = threading.local()


class WriterBusyError(RuntimeError):
    """Another process owns the selected local root."""


@contextmanager
def writer_lock(root: Path):
    paths = ProjectPaths.from_root(root)
    for directory in (
        paths.warehouse,
        paths.data,
        paths.raw,
        paths.parquet,
        paths.state,
        paths.state / "trade",
        paths.state / "replacements",
        paths.state / "dbt",
        paths.state / "dbt-direct",
    ):
        if directory.resolve() != directory:
            raise ValueError(
                "Runtime directories must stay inside the canonical root without symlinks"
            )
    database = paths.warehouse / "coffee_cocoa.duckdb"
    if database.is_symlink():
        raise ValueError("The mutable warehouse must not be a symlink")
    held = getattr(_local, "held", {})
    if paths.root in held:
        yield held[paths.root]
        return
    paths.state.mkdir(parents=True, exist_ok=True)
    # Never unlink this file: all contenders must address the same inode.
    with (paths.state / "writer.lock").open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise WriterBusyError(
                f"Writer busy for {paths.root}; retry after it exits"
            ) from exc
        _local.held = {**held, paths.root: handle.fileno()}
        try:
            yield handle.fileno()
        finally:
            _local.held = held
            # Closing, not LOCK_UN: an inherited fd must retain the lock if a
            # parent dies while its dbt subprocess is still running.


def dbt_environment(root: Path, fd: int, database: Path | None = None) -> dict:
    paths = ProjectPaths.from_root(root)
    return dict(
        os.environ,
        COFFEE_COCOA_HOME=str(paths.root),
        COFFEE_COCOA_DUCKDB_PATH=str(
            database or paths.warehouse / "coffee_cocoa.duckdb"
        ),
        COFFEE_COCOA_LOCK_FD=str(fd),
        COFFEE_COCOA_LOCK_PATH=str(paths.state / "writer.lock"),
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
    )


def run_dbt(root: Path, args: list[str], *, database: Path | None = None, timeout=480):
    """Run dbt with an inherited lock and a finite process deadline."""
    with writer_lock(root) as fd:
        return subprocess.run(
            [str(Path(sys.executable).with_name("dbt")), *args],
            env=dbt_environment(root, fd, database),
            pass_fds=tuple(_local.held.values()),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )


@resource
def local_writer(context):
    """Hold one lock for the complete in-process asset job, including checks."""
    paths = ProjectPaths.from_root()
    with writer_lock(paths.root) as fd:
        paths.warehouse.mkdir(parents=True, exist_ok=True)
        yield fd


def coordinated_capture(function):
    """Coordinate public Python publication helpers as well as their CLI callers."""
    import functools
    import inspect

    signature = inspect.signature(function)

    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        paths = signature.bind(*args, **kwargs).arguments["paths"]
        with writer_lock(paths.root):
            paths.require_capture_root()
            return function(*args, **kwargs)

    return wrapped


def stream_dbt(context, selection: str):
    """Stream dagster-dbt events while the dbt child owns the inherited lock."""
    import json
    import uuid
    from importlib.metadata import version

    from dagster_dbt import DbtCliInvocation
    from packaging.version import Version

    from coffee_cocoa_platform.catalog_metadata import (
        DBT_DIR,
        GovernedDbtTranslator,
        ensure_manifest,
    )

    require_in_process(context)
    paths = ProjectPaths.from_root()
    paths.require_capture_root()
    with writer_lock(paths.root) as fd:
        target = paths.state / "dbt" / uuid.uuid4().hex
        environment = dbt_environment(paths.root, fd)
        environment.update(
            DBT_LOG_FORMAT="json", DBT_TARGET_PATH=str(target), DBT_LOG_PATH=str(target)
        )
        manifest = json.loads(ensure_manifest().read_text())
        translator = GovernedDbtTranslator()
        selected = [
            node["name"]
            for node in manifest["nodes"].values()
            if node["resource_type"] in {"model", "seed"}
            and translator.get_asset_key(node) in context.selected_asset_keys
        ]
        process = subprocess.Popen(
            [
                str(Path(sys.executable).with_name("dbt")),
                "build",
                "--project-dir",
                str(DBT_DIR),
                "--profiles-dir",
                str(DBT_DIR),
                "--select",
                " ".join(selected) or selection,
            ],
            env=environment,
            pass_fds=(fd,),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        timer = threading.Timer(480, process.kill)
        timer.start()
        try:
            invocation = DbtCliInvocation(
                process=process,
                manifest=manifest,
                dagster_dbt_translator=translator,
                project_dir=DBT_DIR,
                target_path=target,
                raise_on_error=True,
                cli_version=Version(version("dbt-core")),
                context=context,
            )
            yield from invocation.stream()
        finally:
            timer.cancel()
            if process.poll() is None:
                process.kill()
            process.wait()


@contextmanager
def run_instance(root):
    """Persist CLI run history in local SQLite using Dagster's standard instance."""
    from dagster import DagsterInstance

    directory = ProjectPaths.from_root(root).state / "dagster"
    directory.mkdir(parents=True, exist_ok=True)
    with DagsterInstance.local_temp(
        str(directory), overrides={"telemetry": {"enabled": False}}
    ) as instance:
        yield instance


def require_in_process(context):
    if context.job_def.executor_def.name not in {
        "in_process",
        "execute_in_process_executor",
    }:
        raise ValueError("Local warehouse jobs require the in_process executor")


_environment_lock = threading.Lock()


def coordinated_run(function):
    """Keep process-global Dagster/dbt environment selection stable across threads."""
    import functools

    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        if not _environment_lock.acquire(blocking=False):
            raise WriterBusyError("Another pipeline is running in this Python process")
        keys = ("COFFEE_COCOA_HOME", "COFFEE_COCOA_DUCKDB_PATH")
        previous = {key: os.environ.get(key) for key in keys}
        try:
            return function(*args, **kwargs)
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            _environment_lock.release()

    return wrapped
