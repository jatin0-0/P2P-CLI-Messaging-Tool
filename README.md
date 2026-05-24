# P2P-CLI-Messaging-Tool
P2P CLI Messaging Tool
P2P CLI Messaging Tool
======================
Terminal chat · multi-peer · persistent history · OS notifications · no bind errors


QUICK START
-----------

Terminal 1 — rendezvous server (run once, leave running)
  python rendezvous_server.py

Terminal 2 — alice
  python tui.py alice bob

Terminal 3 — bob (different port — same machine only)
  python tui.py bob alice --port 9001

On separate machines, both peers use the default port — no --port flag needed.

Connect to multiple peers at once:
  python tui.py alice bob carol

Add a peer while the app is running:
  /add dave


CLONE & INSTALL
---------------

  git clone https://github.com/jatin0-0/P2P-CLI-Messaging-Tool.git
  cd P2P-CLI-Messaging-Tool

  python -m venv venv
  venv\Scripts\activate        # Windows
  source venv/bin/activate     # macOS / Linux

  pip install textual aiosqlite aiohttp flask plyer

Note: plyer is optional (OS notifications). flask only needed on the rendezvous server machine.

Windows filename warning — rename if needed:
  Rename-Item PeerManager.py peer_manager.py
  Rename-Item PeerWorker.py  peer_worker.py


USAGE
-----

Cross-network (different Wi-Fi / mobile hotspot):
  Edit config.py:  RENDEZVOUS_HOST = "YOUR_SERVER_PUBLIC_IP"
  Deploy rendezvous_server.py on a VPS.
  Both peers run tui.py normally.

In-app commands:
  /add <peer_id>   connect to a new peer without restarting
  /quit            exit cleanly
  Ctrl+C           also exits
  Ctrl+N           focus the message input

Connection paths (automatic, no flags needed):
  Path A — direct LAN      same /24 subnet (e.g. both on 192.168.1.x)
  Path B — UDP hole punch  different networks, cone NAT, 3s timeout
  Path C — HTTP relay      symmetric NAT or no public IP

History: all messages stored in chat.db, loads on startup automatically.


ARCHITECTURE
------------

One UDP socket bound once by peer_manager.py for the entire process lifetime.
All peers share it. Packets routed by sender's (ip, port) — bind errors impossible.

  tui.py
    └── peer_manager.py  (binds 0.0.0.0:9000 once)
          ├── peer_worker: bob    (asyncio task)
          ├── peer_worker: carol  (asyncio task)
          └── peer_worker: dave   (asyncio task)
                └── storage.py   (SQLite — messages + peer records)


FILES
-----

  tui.py                entry point — run this
  rendezvous_server.py  entry point — run this on your server
  peer_manager.py       owns the UDP socket, routes packets
  peer_worker.py        one asyncio task per peer
  storage.py            SQLite persistence via aiosqlite
  stun_client.py        STUN public IP discovery (unchanged)
  config.py             all tunable constants
  chat.db               created automatically, do not commit to git


CONFIG (config.py)
------------------

  RENDEZVOUS_HOST      127.0.0.1   change to VPS IP for cross-network
  RENDEZVOUS_PORT      5000        rendezvous server port
  LOCAL_UDP_PORT       9000        default UDP port (one per machine)
  DB_PATH              chat.db     SQLite file location
  KEEPALIVE_INTERVAL   10          seconds between PING packets
  RECONNECT_DELAY      5           seconds before reconnect after silence
  MAX_HISTORY_DISPLAY  200         messages loaded on tab open
  LONG_POLL_TIMEOUT    30          rendezvous long-poll timeout (seconds)


github.com/jatin0-0/P2P-CLI-Messaging-Tool
