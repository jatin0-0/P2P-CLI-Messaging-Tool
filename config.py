STUN_HOST = "stun.l.google.com"
STUN_PORT = 19302

RENDEZVOUS_HOST = "127.0.0.1"   # change to your server's public IP for cross-network
RENDEZVOUS_PORT = 5000

LONG_POLL_TIMEOUT = 30           # seconds the rendezvous server holds a request

LOCAL_UDP_PORT    = 9000         # single shared UDP port for all peers

DB_PATH           = "chat.db"   # SQLite file, created automatically on first run

KEEPALIVE_INTERVAL = 10          # seconds between UDP keep-alive pings
RECONNECT_DELAY    = 5           # seconds before retrying a lost connection
MAX_HISTORY_DISPLAY = 200        # messages loaded from DB on tab open
