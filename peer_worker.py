"""
peer_worker.py — one asyncio task per remote peer.

Each PeerWorker:
  • negotiates the best path  (LAN → hole punch → relay)
  • owns an asyncio.Queue for inbound packets routed to it by PeerManager
  • emits MessageReceived events up to the TUI
  • persists messages via Storage
  • sends keep-alives and reconnects on silence
"""

import asyncio
import time
import aiohttp
from dataclasses import dataclass
from typing import Callable, Awaitable

from config import (
    RENDEZVOUS_HOST, RENDEZVOUS_PORT, LONG_POLL_TIMEOUT,
    KEEPALIVE_INTERVAL, RECONNECT_DELAY
)

RENDEZVOUS_URL = f"http://{RENDEZVOUS_HOST}:{RENDEZVOUS_PORT}"


# ── Events emitted to TUI ─────────────────────────────────────────────────────

@dataclass
class MessageReceived:
    peer_id: str
    body: str
    ts: str

@dataclass
class PeerStatusChanged:
    peer_id: str
    status: str          # "connecting" | "online" | "relay" | "offline"
    path: str            # "lan" | "holepunch" | "relay" | "none"


# ── Helper ────────────────────────────────────────────────────────────────────

def same_subnet(ip1: str, ip2: str) -> bool:
    return ip1.rsplit(".", 1)[0] == ip2.rsplit(".", 1)[0]


# ── PeerWorker ────────────────────────────────────────────────────────────────

class PeerWorker:
    """
    Manages the full lifecycle of a connection to one remote peer.

    Parameters
    ----------
    my_id        : our username
    peer_id      : remote peer's username
    my_lan_ip    : our LAN IP (discovered at startup)
    my_public_ip : our public IP from STUN (may be None)
    my_public_port: our public port from STUN
    send_udp     : coroutine(data: bytes, ip: str, port: int) — send via shared socket
    on_event     : coroutine(event) — deliver MessageReceived / PeerStatusChanged to TUI
    storage      : Storage instance for persistence
    """

    def __init__(self, my_id, peer_id, my_lan_ip,
                 my_public_ip, my_public_port,
                 udp_port, send_udp, on_event, storage):
        self.my_id          = my_id
        self.peer_id        = peer_id
        self.my_lan_ip      = my_lan_ip
        self.my_public_ip   = my_public_ip
        self.my_public_port = my_public_port
        self.udp_port       = udp_port

        self._send_udp  = send_udp
        self._on_event  = on_event
        self._storage   = storage

        # Inbound packet queue — PeerManager drops packets here
        self.inbound: asyncio.Queue[bytes] = asyncio.Queue()

        # Current peer address for direct UDP (None if relay)
        self._peer_ip: str | None = None
        self._peer_port: int | None = None
        self._path = "none"

        # Relay session id (same on both sides)
        self._session_id = "_".join(sorted([my_id, peer_id]))

        self._last_recv  = 0.0
        self._running    = True
        self._task: asyncio.Task | None = None

        self._relay_send_queue: asyncio.Queue[str] = asyncio.Queue()

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> asyncio.Task:
        self._task = asyncio.create_task(self._run(), name=f"worker-{self.peer_id}")
        return self._task

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    async def send_message(self, text: str):
        """Called by TUI to send a message to this peer."""
        ts = await self._storage.save_message(self.peer_id, "outgoing", text)
        if self._path in ("lan", "holepunch") and self._peer_ip:
            await self._send_udp(text.encode(), self._peer_ip, self._peer_port)
        elif self._path == "relay":
            await self._relay_send_queue.put(text)
        else:
            # Queue it — will be sent once connected
            await self._relay_send_queue.put(text)

    # ── Internal lifecycle ────────────────────────────────────────────────────

    async def _run(self):
        while self._running:
            try:
                await self._connect_and_chat()
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self._emit(PeerStatusChanged(self.peer_id, "offline", "none"))
                await asyncio.sleep(RECONNECT_DELAY)

    async def _connect_and_chat(self):
        await self._emit(PeerStatusChanged(self.peer_id, "connecting", "none"))

        async with aiohttp.ClientSession() as session:
            peer_info = await self._wait_for_peer(session)

        peer_lan_ip    = peer_info["lan_ip"]
        peer_lan_port  = int(peer_info["lan_port"])
        peer_pub_ip    = peer_info.get("public_ip")
        peer_pub_port  = int(peer_info["public_port"]) if peer_info.get("public_port") else None

        # ── Path selection ────────────────────────────────────────────────────
        if same_subnet(self.my_lan_ip, peer_lan_ip):
            self._peer_ip   = peer_lan_ip
            self._peer_port = peer_lan_port
            self._path      = "lan"
            await self._emit(PeerStatusChanged(self.peer_id, "online", "lan"))
            await self._storage.upsert_peer(
                self.peer_id, peer_lan_ip, peer_lan_port,
                peer_pub_ip, peer_pub_port, path="lan"
            )
            await self._direct_chat_loop()

        elif peer_pub_ip and self.my_public_ip:
            # Try hole punch
            success = await self._try_hole_punch(peer_pub_ip, peer_pub_port)
            if success:
                self._peer_ip   = peer_pub_ip
                self._peer_port = peer_pub_port
                self._path      = "holepunch"
                await self._emit(PeerStatusChanged(self.peer_id, "online", "holepunch"))
                await self._storage.upsert_peer(
                    self.peer_id, peer_lan_ip, peer_lan_port,
                    peer_pub_ip, peer_pub_port, path="holepunch"
                )
                await self._direct_chat_loop()
            else:
                self._path = "relay"
                await self._emit(PeerStatusChanged(self.peer_id, "online", "relay"))
                await self._storage.upsert_peer(
                    self.peer_id, peer_lan_ip, peer_lan_port,
                    peer_pub_ip, peer_pub_port, path="relay"
                )
                await self._relay_chat_loop()
        else:
            self._path = "relay"
            await self._emit(PeerStatusChanged(self.peer_id, "online", "relay"))
            await self._storage.upsert_peer(
                self.peer_id, peer_lan_ip, peer_lan_port,
                peer_pub_ip, peer_pub_port, path="relay"
            )
            await self._relay_chat_loop()

    # ── Rendezvous ────────────────────────────────────────────────────────────

    async def _wait_for_peer(self, session: aiohttp.ClientSession) -> dict:
        while True:
            try:
                async with session.get(
                    f"{RENDEZVOUS_URL}/peer/{self.peer_id}",
                    timeout=aiohttp.ClientTimeout(total=LONG_POLL_TIMEOUT + 5)
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    # 408 timeout — retry
            except aiohttp.ClientError:
                await asyncio.sleep(2)

    # ── Hole punch ────────────────────────────────────────────────────────────

    async def _try_hole_punch(self, ip: str, port: int, timeout: float = 3.0) -> bool:
        for _ in range(5):
            await self._send_udp(f"HELLO:{self.my_id}".encode(), ip, port)
            await asyncio.sleep(0.2)

        try:
            data = await asyncio.wait_for(self.inbound.get(), timeout=timeout)
            msg = data.decode(errors="replace")
            return True  # any response = hole punch success
        except asyncio.TimeoutError:
            return False

    # ── Direct UDP chat loop ──────────────────────────────────────────────────

    async def _direct_chat_loop(self):
        self._last_recv = time.monotonic()

        keepalive_task = asyncio.create_task(self._keepalive_loop())
        try:
            while self._running:
                try:
                    data = await asyncio.wait_for(self.inbound.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    # Check for silence timeout
                    if time.monotonic() - self._last_recv > KEEPALIVE_INTERVAL * 4:
                        raise ConnectionError("Peer silent — reconnecting")
                    continue

                text = data.decode(errors="replace")
                if text.startswith("HELLO:") or text == "PING":
                    await self._send_udp(b"PONG", self._peer_ip, self._peer_port)
                    self._last_recv = time.monotonic()
                    continue
                if text == "PONG":
                    self._last_recv = time.monotonic()
                    continue

                self._last_recv = time.monotonic()
                ts = await self._storage.save_message(self.peer_id, "incoming", text)
                await self._emit(MessageReceived(self.peer_id, text, ts))
        finally:
            keepalive_task.cancel()

    async def _keepalive_loop(self):
        while self._running and self._peer_ip:
            await asyncio.sleep(KEEPALIVE_INTERVAL)
            try:
                await self._send_udp(b"PING", self._peer_ip, self._peer_port)
            except Exception:
                break

    # ── Relay chat loop ───────────────────────────────────────────────────────

    async def _relay_chat_loop(self):
        recv_task = asyncio.create_task(self._relay_recv_loop())
        send_task = asyncio.create_task(self._relay_send_loop())
        try:
            await asyncio.gather(recv_task, send_task)
        finally:
            recv_task.cancel()
            send_task.cancel()

    async def _relay_recv_loop(self):
        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    async with session.get(
                        f"{RENDEZVOUS_URL}/relay/recv/{self._session_id}/{self.my_id}",
                        timeout=aiohttp.ClientTimeout(total=LONG_POLL_TIMEOUT + 5)
                    ) as resp:
                        if resp.status == 200:
                            body = await resp.text()
                            ts = await self._storage.save_message(
                                self.peer_id, "incoming", body
                            )
                            await self._emit(MessageReceived(self.peer_id, body, ts))
                except Exception:
                    await asyncio.sleep(1)

    async def _relay_send_loop(self):
        async with aiohttp.ClientSession() as session:
            while self._running:
                text = await self._relay_send_queue.get()
                try:
                    async with session.post(
                        f"{RENDEZVOUS_URL}/relay/send/{self._session_id}/{self.my_id}",
                        data=text.encode(),
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as _:
                        pass
                except Exception:
                    # Re-queue on failure
                    await self._relay_send_queue.put(text)
                    await asyncio.sleep(1)

    # ── Event helper ─────────────────────────────────────────────────────────

    async def _emit(self, event):
        await self._on_event(event)