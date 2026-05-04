from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DashboardProcessInfo:
    host: str
    port: int
    url: str
    pid: int | None
    dataset: str | None
    log_path: Path
    state_path: Path
    healthy: bool
    running: bool


def dashboard_state_path(port: int) -> Path:
    return Path(tempfile.gettempdir()) / f"eu5_dashboard_{port}.json"


def dashboard_log_path(port: int) -> Path:
    return Path(tempfile.gettempdir()) / f"eu5_dashboard_{port}.log"


def dashboard_error_log_path(port: int) -> Path:
    return Path(tempfile.gettempdir()) / f"eu5_dashboard_{port}.err.log"


def dashboard_status(*, host: str = "127.0.0.1", port: int = 8050) -> DashboardProcessInfo:
    state_path = dashboard_state_path(port)
    state = _read_state(state_path)
    pid = _safe_int(state.get("pid"))
    url = state.get("url") or f"http://{host}:{port}"
    return DashboardProcessInfo(
        host=str(state.get("host") or host),
        port=int(state.get("port") or port),
        url=str(url),
        pid=pid,
        dataset=state.get("dataset"),
        log_path=Path(state.get("log_path") or dashboard_log_path(port)),
        state_path=state_path,
        healthy=_health_check(str(url)),
        running=False if pid is None else _pid_running(pid),
    )


def start_dashboard_process(
    *,
    dataset: str | Path,
    profile: str = "merged_default",
    load_order_path: str | Path | None = None,
    host: str = "127.0.0.1",
    port: int = 8050,
    timeout_seconds: float = 20.0,
    refresh_ms: int = 5000,
) -> DashboardProcessInfo:
    existing = dashboard_status(host=host, port=port)
    if existing.healthy and _same_path(existing.dataset, dataset):
        return existing
    if existing.pid is not None and existing.running:
        stop_dashboard_process(port=port)

    state_path = dashboard_state_path(port)
    log_path = dashboard_log_path(port)
    error_log_path = dashboard_error_log_path(port)
    launcher = _launcher_code(
        dataset=dataset,
        profile=profile,
        load_order_path=load_order_path,
        host=host,
        port=port,
        refresh_ms=refresh_ms,
    )
    command = [
        sys.executable,
        "-c",
        launcher,
    ]

    log_path.parent.mkdir(parents=True, exist_ok=True)
    stdout = log_path.open("w", encoding="utf-8")
    stderr = error_log_path.open("w", encoding="utf-8")
    try:
        process = subprocess.Popen(  # noqa: S603
            command,
            cwd=Path.cwd(),
            stdout=stdout,
            stderr=stderr,
            stdin=subprocess.DEVNULL,
            creationflags=_creation_flags(),
            close_fds=True,
        )
    finally:
        stdout.close()
        stderr.close()

    url = f"http://{host}:{port}"
    state = {
        "pid": process.pid,
        "host": host,
        "port": port,
        "url": url,
        "dataset": str(dataset),
        "profile": profile,
        "load_order_path": "" if load_order_path is None else str(load_order_path),
        "refresh_ms": int(refresh_ms),
        "log_path": str(log_path),
        "error_log_path": str(error_log_path),
        "command": command,
        "started_at": time.time(),
    }
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if process.poll() is not None:
            break
        if _health_check(url):
            return dashboard_status(host=host, port=port)
        time.sleep(0.25)
    return dashboard_status(host=host, port=port)


def stop_dashboard_process(*, port: int = 8050) -> DashboardProcessInfo:
    info = dashboard_status(port=port)
    if info.pid is not None and info.running:
        _terminate_pid(info.pid)
    if info.state_path.exists():
        info.state_path.unlink()
    return dashboard_status(port=port)


def _read_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _health_check(url: str, *, timeout_seconds: float = 1.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
            return 200 <= response.status < 500
    except (OSError, urllib.error.URLError):
        return False


def _pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _terminate_pid(pid: int) -> None:
    if not _pid_running(pid):
        return
    if os.name == "nt":
        subprocess.run(  # noqa: S603
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return
    try:
        os.kill(pid, 15)
    except OSError:
        return


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _same_path(left: str | Path | None, right: str | Path) -> bool:
    if left in {None, ""}:
        return False
    try:
        return Path(left).resolve() == Path(right).resolve()
    except OSError:
        return str(left) == str(right)


def _creation_flags() -> int:
    if os.name != "nt":
        return 0
    return subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS


def _launcher_code(
    *,
    dataset: str | Path,
    profile: str,
    load_order_path: str | Path | None,
    host: str,
    port: int,
    refresh_ms: int,
) -> str:
    load_order = "None" if load_order_path is None else f"Path({str(load_order_path)!r})"
    return (
        "from pathlib import Path\n"
        "from eu5gameparser.savegame.dashboard import run_dashboard\n"
        "run_dashboard("
        f"Path({str(dataset)!r}), "
        f"profile={profile!r}, "
        f"load_order_path={load_order}, "
        f"host={host!r}, "
        f"port={int(port)}, "
        f"refresh_ms={int(refresh_ms)}"
        ")\n"
    )
