"""
JSON-RPC 2.0 client for the marmot-cli daemon over TCP.
"""

import asyncio
import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class MarmotRpcError(Exception):
    """Raised when the daemon returns a JSON-RPC error response."""


class MarmotRpcClient:
    """TCP-based JSON-RPC client for marmot-cli daemon."""

    def __init__(self, host: str = "127.0.0.1", port: int = 9222):
        self.host = host
        self.port = port

    async def ping(self, timeout: float = 5.0) -> dict:
        return await self._rpc("ping", {}, timeout)

    async def identity_npub(self) -> dict:
        return await self._rpc("identity_npub", {})

    async def list_groups(self) -> dict:
        return await self._rpc("list_groups", {})

    async def send_message(
        self, group_id: str, content: str, publish: bool = True
    ) -> dict:
        return await self._rpc(
            "send_message",
            {"group_id": group_id, "content": content, "publish": publish},
            timeout=30.0,
        )

    async def send_reaction(
        self, group_id: str, target_event_id: str, emoji: str, publish: bool = True
    ) -> dict:
        return await self._rpc(
            "send_reaction",
            {
                "group_id": group_id,
                "target_event_id": target_event_id,
                "emoji": emoji,
                "publish": publish,
            },
        )

    async def receive(self, timeout: float = 120.0) -> dict:
        return await self._rpc("receive", {}, timeout)

    async def _rpc(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        timeout: float = 10.0,
    ) -> dict:
        request_id = int(asyncio.get_event_loop().time() * 1000)
        request = (
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params or {},
                }
            )
            + "\n"
        )

        reader: Optional[asyncio.StreamReader] = None
        writer: Optional[asyncio.StreamWriter] = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=min(timeout, 5.0),
            )
            writer.write(request.encode("utf-8"))
            await writer.drain()

            response_data = await asyncio.wait_for(
                reader.readuntil(b"\n"), timeout=timeout
            )
            response = json.loads(response_data.decode("utf-8").strip())

            if not isinstance(response, dict):
                raise MarmotRpcError(f"Unexpected response type: {type(response)}")

            if "error" in response:
                err = response["error"]
                msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                raise MarmotRpcError(f"{method} RPC error: {msg}")

            return response.get("result", {})

        except asyncio.TimeoutError:
            raise MarmotRpcError(f"{method} RPC timed out after {timeout}s")
        except (ConnectionRefusedError, OSError) as e:
            raise MarmotRpcError(f"{method} RPC connection failed: {e}")
        except json.JSONDecodeError as e:
            raise MarmotRpcError(f"{method} RPC invalid JSON: {e}")
        finally:
            if writer is not None:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
