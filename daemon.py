"""
marmot-cli daemon lifecycle management.

Spawns the daemon as a subprocess, waits for it to become healthy,
and provides auto-restart with exponential backoff.
"""

import asyncio
import logging
import time
from asyncio.subprocess import Process
from typing import Optional

from .rpc_client import MarmotRpcClient

logger = logging.getLogger(__name__)

BACKOFF_INITIAL_MS = 1000
BACKOFF_MAX_MS = 10000
BACKOFF_FACTOR = 2
BACKOFF_JITTER = 0.2


def _compute_backoff(attempts: int) -> float:
    base = min(
        BACKOFF_INITIAL_MS * (BACKOFF_FACTOR ** attempts),
        BACKOFF_MAX_MS,
    )
    jitter = base * BACKOFF_JITTER * (time.time() % 1)
    return (base + jitter) / 1000.0


async def wait_for_daemon_ready(
    client: MarmotRpcClient,
    timeout: float = 30.0,
    log: Optional[callable] = None,
) -> None:
    """Poll ping until the daemon responds or timeout expires."""
    deadline = time.monotonic() + timeout
    last_log_at = 0.0
    while time.monotonic() < deadline:
        try:
            result = await client.ping(timeout=3.0)
            if result.get("pong"):
                if log:
                    log("marmot daemon ready")
                return
        except Exception:
            pass
        now = time.monotonic()
        if log and now - last_log_at > 10.0:
            log("waiting for marmot daemon to become ready...")
            last_log_at = now
        await asyncio.sleep(1.0)
    raise TimeoutError(f"marmot daemon did not become ready within {timeout}s")


async def spawn_daemon(
    cli_path: str,
    host: str = "127.0.0.1",
    port: int = 9222,
) -> Process:
    """Spawn marmot-cli daemon as a subprocess."""
    proc = await asyncio.create_subprocess_exec(
        cli_path,
        "daemon",
        "--listen", f"{host}:{port}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    return proc


async def _log_stream(stream: Optional[asyncio.StreamReader], prefix: str, log_fn: callable) -> None:
    """Read lines from a subprocess stream and forward them to the logger."""
    if stream is None:
        return
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip("\n")
            if text:
                log_fn(f"[marmot-daemon] {prefix}: {text}")
    except Exception:
        pass


async def monitor_daemon(
    proc: Process,
    client: MarmotRpcClient,
    restart_fn: callable,
    log_info: Optional[callable] = None,
    log_error: Optional[callable] = None,
) -> None:
    """Monitor a running daemon process and auto-restart on crash.

    Reads stdout/stderr until the process exits, then calls restart_fn.
    """
    info_fn = log_info or logger.info
    err_fn = log_error or logger.error

    async def _stdout_reader():
        await _log_stream(proc.stdout, "stdout", info_fn)

    async def _stderr_reader():
        await _log_stream(proc.stderr, "stderr", err_fn)

    reader_tasks = [
        asyncio.create_task(_stdout_reader()),
        asyncio.create_task(_stderr_reader()),
    ]

    try:
        exit_code = await proc.wait()
        info_fn(f"marmot daemon exited with code {exit_code}")
    finally:
        for t in reader_tasks:
            t.cancel()
        await asyncio.gather(*reader_tasks, return_exceptions=True)

    if exit_code != 0:
        err_fn(f"marmot daemon crashed (exit code {exit_code}), restarting...")
        await restart_fn()
