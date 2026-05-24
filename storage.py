"""
storage.py — async SQLite persistence layer.

Tables:
  peers    — every peer we've ever spoken to (last seen, addresses)
  messages — every message sent or received, per peer

Usage:
  db = await Storage.connect()
  await db.save_message("bob", "outgoing", "hey!")
  msgs = await db.get_history("bob")
"""

import aiosqlite
from datetime import datetime
from config import DB_PATH


class Storage:
    def __init__(self, conn: aiosqlite.Connection):
        self._conn = conn

    @classmethod
    async def connect(cls) -> "Storage":
        conn = await aiosqlite.connect(DB_PATH)
        conn.row_factory = aiosqlite.Row
        await conn.executescript("""
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS peers (
                peer_id      TEXT PRIMARY KEY,
                lan_ip       TEXT,
                lan_port     INTEGER,
                public_ip    TEXT,
                public_port  INTEGER,
                last_seen    TEXT,
                path         TEXT DEFAULT 'unknown'
            );

            CREATE TABLE IF NOT EXISTS messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                peer_id     TEXT NOT NULL,
                direction   TEXT NOT NULL CHECK(direction IN ('incoming','outgoing')),
                body        TEXT NOT NULL,
                ts          TEXT NOT NULL,
                delivered   INTEGER DEFAULT 1
            );

            CREATE INDEX IF NOT EXISTS idx_messages_peer
                ON messages(peer_id, id);
        """)
        await conn.commit()
        return cls(conn)

    # ── Peers ─────────────────────────────────────────────────────────────────

    async def upsert_peer(self, peer_id: str, lan_ip: str = None,
                          lan_port: int = None, public_ip: str = None,
                          public_port: int = None, path: str = None):
        """Insert or update a peer's address record."""
        await self._conn.execute("""
            INSERT INTO peers (peer_id, lan_ip, lan_port, public_ip, public_port, last_seen, path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(peer_id) DO UPDATE SET
                lan_ip      = COALESCE(excluded.lan_ip,      lan_ip),
                lan_port    = COALESCE(excluded.lan_port,    lan_port),
                public_ip   = COALESCE(excluded.public_ip,   public_ip),
                public_port = COALESCE(excluded.public_port, public_port),
                last_seen   = excluded.last_seen,
                path        = COALESCE(excluded.path,        path)
        """, (peer_id, lan_ip, lan_port, public_ip, public_port,
              datetime.now().isoformat(timespec="seconds"), path))
        await self._conn.commit()

    async def get_known_peers(self) -> list[dict]:
        """Return all peers we've ever connected to."""
        async with self._conn.execute(
            "SELECT * FROM peers ORDER BY last_seen DESC"
        ) as cur:
            return [dict(row) for row in await cur.fetchall()]

    # ── Messages ──────────────────────────────────────────────────────────────

    async def save_message(self, peer_id: str, direction: str, body: str):
        """Persist a single message. direction = 'incoming' | 'outgoing'"""
        ts = datetime.now().isoformat(timespec="seconds")
        await self._conn.execute(
            "INSERT INTO messages (peer_id, direction, body, ts) VALUES (?,?,?,?)",
            (peer_id, direction, body, ts)
        )
        await self._conn.commit()
        return ts

    async def get_history(self, peer_id: str, limit: int = 200) -> list[dict]:
        """Return the last `limit` messages with a peer, oldest first."""
        async with self._conn.execute("""
            SELECT direction, body, ts FROM messages
            WHERE peer_id = ?
            ORDER BY id DESC LIMIT ?
        """, (peer_id, limit)) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in reversed(rows)]

    async def unread_count(self, peer_id: str, since_ts: str) -> int:
        """Count incoming messages from peer_id after since_ts."""
        async with self._conn.execute("""
            SELECT COUNT(*) FROM messages
            WHERE peer_id = ? AND direction = 'incoming' AND ts > ?
        """, (peer_id, since_ts)) as cur:
            row = await cur.fetchone()
        return row[0] if row else 0

    async def close(self):
        await self._conn.close()