"""
peer_manager.py — the single UDP socket owner.

Responsibilities:
  • Bind ONE UDP socket for the entire app lifetime
  • Discover our LAN + public addresses (STUN)
  • Register with the rendezvous server
  • Route incoming packets to the correct PeerWorker by (ip, port)
  • Spawn / stop PeerWorker tasks on demand

Usage:
  manager = await PeerManager.create(my_id, udp_port, on_event, storage)
  await manager.add_peer("bob")
  await manager.send_to("bob", "hello!")
  await manager.run_forever()   # blocks — reads socket in a loop
"""

import asyncio
import socket
import aiohttp
from typing import Callable, Awaitable

from stun_client import get_public_address
from storage import Storage
from peer_worker import PeerWorker, PeerStatusChanged
from config import RENDEZVOUS_HOST, RENDEZVOUS_PORT, LOCAL_UDP_PORT


RENDEZVOUS_URL = f"http://{RENDEZVOUS_HOST}:{RENDEZVOUS_PORT}"


def get_lan_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


class PeerManager:
    def __init__(self, my_id: str, udp_port: int,
                 on_event: Callable, storage: Storage):
        self.my_id    = my_id
        self.udp_port = udp_port
        self._on_event = on_event
        self._storage  = storage

        self.my_lan_ip     = get_lan_ip()
        self.my_public_ip  = None
        self.my_public_port= None

        # Transport/protocol set in create()
        self._transport = None
        self._protocol  = None

        # peer_id → PeerWorker
        self._workers: dict[str, PeerWorker] = {}

        # (ip, port) → peer_id  — reverse-lookup for routing
        self._addr_map: dict[tuple, str] = {}

    @classmethod
    async def create(cls, my_id: str, udp_port: int,
                     on_event: Callable, storage: Storage) -> "PeerManager":
        mgr = cls(my_id, udp_port, on_event, storage)

        # STUN
        try:
            mgr.my_public_ip, mgr.my_public_port = get_public_address()
        except Exception as e:
            pass   # LAN-only mode

        # Bind UDP socket via asyncio DatagramProtocol
        loop = asyncio.get_event_loop()
        mgr._transport, mgr._protocol = await loop.create_datagram_endpoint(
            lambda: _UDPProtocol(mgr._route_packet),
            local_addr=("0.0.0.0", udp_port)
        )

        # Register with rendezvous
        await mgr._register()

        return mgr

    async def _register(self):
        payload = {
            "peer_id":  self.my_id,
            "lan_ip":   self.my_lan_ip,
            "lan_port": self.udp_port,
        }
        if self.my_public_ip:
            payload["public_ip"]   = self.my_public_ip
            payload["public_port"] = self.my_public_port

        async with aiohttp.ClientSession() as s:
            async with s.post(f"{RENDEZVOUS_URL}/register", json=payload) as r:
                r.raise_for_status()

    # ── Worker management ─────────────────────────────────────────────────────

    async def add_peer(self, peer_id: str):
        """Spawn a PeerWorker for peer_id if not already running."""
        if peer_id in self._workers:
            return
        worker = PeerWorker(
            my_id         = self.my_id,
            peer_id       = peer_id,
            my_lan_ip     = self.my_lan_ip,
            my_public_ip  = self.my_public_ip,
            my_public_port= self.my_public_port,
            udp_port      = self.udp_port,
            send_udp      = self._send_udp,
            on_event      = self._on_event,
            storage       = self._storage,
        )
        self._workers[peer_id] = worker
        worker.start()

    async def remove_peer(self, peer_id: str):
        if peer_id in self._workers:
            self._workers[peer_id].stop()
            del self._workers[peer_id]

    async def send_to(self, peer_id: str, text: str):
        if peer_id in self._workers:
            await self._workers[peer_id].send_message(text)

    def get_worker(self, peer_id: str) -> PeerWorker | None:
        return self._workers.get(peer_id)

    # ── UDP I/O ───────────────────────────────────────────────────────────────

    async def _send_udp(self, data: bytes, ip: str, port: int):
        if self._transport:
            self._transport.sendto(data, (ip, port))
            # Register address → worker mapping for routing
            # (worker calls this with its peer's address after hole punch)

    def register_addr(self, peer_id: str, ip: str, port: int):
        """Called by PeerWorker once it knows its peer's address."""
        self._addr_map[(ip, port)] = peer_id

    def _route_packet(self, data: bytes, addr: tuple):
        """Called by the UDP protocol for every incoming datagram."""
        ip, port = addr

        # Try exact address match first
        peer_id = self._addr_map.get((ip, port))

        # Fallback: scan workers for anyone whose peer_ip matches
        if peer_id is None:
            for pid, worker in self._workers.items():
                if worker._peer_ip == ip:
                    peer_id = pid
                    self._addr_map[(ip, port)] = pid
                    break

        # Last resort: if only one worker, give it to them
        if peer_id is None and len(self._workers) == 1:
            peer_id = next(iter(self._workers))

        if peer_id and peer_id in self._workers:
            worker = self._workers[peer_id]
            asyncio.get_event_loop().call_soon_threadsafe(
                worker.inbound.put_nowait, data
            )

    def close(self):
        for w in self._workers.values():
            w.stop()
        if self._transport:
            self._transport.close()


class _UDPProtocol(asyncio.DatagramProtocol):
    """Minimal asyncio UDP protocol — just forwards datagrams to a callback."""
    def __init__(self, callback):
        self._callback = callback

    def datagram_received(self, data: bytes, addr):
        self._callback(data, addr)

    def error_received(self, exc):
        pass   # silently ignore — peer went offline, ICMP unreachable, etc.

    