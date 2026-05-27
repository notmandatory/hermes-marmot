"""Async WebSocket Nostr relay client (NIP-01)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Optional
from uuid import uuid4

import websockets

logger = logging.getLogger(__name__)

RECONNECT_DELAY_S = 5.0
PING_INTERVAL_S = 30.0


class NostrRelayClient:
    def __init__(self, url: str):
        self.url = url
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = False
        self._subs: dict[str, Callable] = {}
        self._listen_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._should_reconnect = True
        self._closed = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        self._closed = False
        self._should_reconnect = True
        await self._do_connect()

    async def _do_connect(self) -> None:
        try:
            self._ws = await websockets.connect(
                self.url,
                ping_interval=PING_INTERVAL_S,
                close_timeout=5,
            )
            self._connected = True
            logger.info("Relay connected: %s", self.url)
            self._listen_task = asyncio.create_task(self._listen())
            self._ping_task = asyncio.create_task(self._ping_loop())
        except Exception as e:
            logger.warning("Relay connect failed %s: %s", self.url, e)
            self._connected = False
            raise

    async def disconnect(self) -> None:
        self._should_reconnect = False
        self._closed = True
        for t in (self._listen_task, self._ping_task, self._reconnect_task):
            if t and not t.done():
                t.cancel()
        self._listen_task = self._ping_task = self._reconnect_task = None
        if self._ws:
            await self._ws.close()
            self._ws = None
        self._connected = False

    async def subscribe(
        self,
        filters: list[dict[str, Any]],
        callback: Callable[[dict[str, Any]], None],
    ) -> str:
        sub_id = uuid4().hex[:8]
        msg = json.dumps(["REQ", sub_id, *filters])
        logger.debug("REQ %s on %s: %s", sub_id, self.url, msg[:200])
        if self._ws:
            await self._ws.send(msg)
        self._subs[sub_id] = callback
        return sub_id

    async def unsubscribe(self, sub_id: str) -> None:
        self._subs.pop(sub_id, None)
        if self._ws:
            await self._ws.send(json.dumps(["CLOSE", sub_id]))

    async def publish(self, event_json: str) -> None:
        event = json.loads(event_json) if isinstance(event_json, str) else event_json
        msg = json.dumps(["EVENT", event])
        if not self._ws:
            logger.warning("Cannot publish, %s not connected", self.url)
            return
        try:
            await self._ws.send(msg)
            logger.info("Published event kind=%s id=%s to %s", event.get("kind"), event.get("id",""), self.url)
        except Exception as e:
            logger.warning("Publish failed on %s: %s", self.url, e)

    async def _listen(self) -> None:
        ws = self._ws
        if not ws:
            return
        try:
            async for raw in ws:
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if not isinstance(data, list) or len(data) < 2:
                    continue
                msg_type = data[0]
                if msg_type == "EVENT":
                    sub_id = data[1]
                    event = data[2] if len(data) > 2 else data[1]
                    cb = self._subs.get(sub_id)
                    if cb:
                        cb(event)
                    else:
                        cb = self._subs.get("*")
                        if cb:
                            cb(event)
                elif msg_type == "EOSE":
                    sub_id = data[1]
                    logger.debug("EOSE %s on %s", sub_id, self.url)
                elif msg_type == "OK":
                    ev_id = data[1]
                    ok = data[2]
                    msg = data[3] if len(data) > 3 else ""
                    if ok:
                        logger.info("Relay %s accepted %s", self.url, ev_id[:16])
                    else:
                        logger.info("Relay %s REJECTED %s: %s", self.url, ev_id[:16], msg)
                elif msg_type == "NOTICE":
                    logger.info("Relay %s notice: %s", self.url, data[1])
        except websockets.ConnectionClosed:
            logger.info("Relay %s connection closed", self.url)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning("Relay %s listen error: %s", self.url, e)
        finally:
            self._connected = False
            if self._should_reconnect and not self._closed:
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _ping_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL_S)
                if self._ws:
                    try:
                        await self._ws.ping()
                    except Exception:
                        break
        except asyncio.CancelledError:
            pass

    async def _reconnect_loop(self) -> None:
        delay = RECONNECT_DELAY_S
        while self._should_reconnect and not self._closed:
            logger.info("Reconnecting to %s in %.0fs...", self.url, delay)
            await asyncio.sleep(delay)
            try:
                await self._do_connect()
                if self._connected:
                    logger.info("Reconnected to %s", self.url)
                    return
            except Exception:
                delay = min(delay * 1.5, 30.0)


class RelayManager:
    def __init__(self, relay_urls: list[str]):
        self.relays: dict[str, NostrRelayClient] = {}
        for url in relay_urls:
            self.relays[url] = NostrRelayClient(url)

    async def connect_all(self) -> None:
        results = await asyncio.gather(
            *[r.connect() for r in self.relays.values()],
            return_exceptions=True,
        )
        connected = sum(1 for r in results if r is None)
        logger.info("Connected to %d/%d relays", connected, len(self.relays))

    async def disconnect_all(self) -> None:
        await asyncio.gather(
            *[r.disconnect() for r in self.relays.values()],
            return_exceptions=True,
        )

    def subscribe_all(
        self,
        filters: list[dict[str, Any]],
        callback: Callable[[dict[str, Any]], None],
    ) -> list[str]:
        sub_ids: list[str] = []
        for r in self.relays.values():
            if r.is_connected:
                sub_id = asyncio.create_task(r.subscribe(filters, callback))
                sub_ids.append(sub_id)
        return sub_ids

    async def publish_all(self, event_json: str) -> None:
        await asyncio.gather(
            *[r.publish(event_json) for r in self.relays.values()],
            return_exceptions=True,
        )

    async def publish_one(self, event_json: str) -> None:
        for r in self.relays.values():
            if r.is_connected:
                await r.publish(event_json)
                return
