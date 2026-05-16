"""
Rendezvous server — run this on a machine both peers can reach.
For local/LAN testing: run it and both peers point to its IP.
For cross-network testing: run on a VPS with a public IP.

Endpoints:
  POST /register              — peer registers its id, LAN address, and public address
  GET  /peer/<peer_id>        — long poll: waits until that peer is registered, returns its info
  POST /relay/send/<sid>/<id> — relay: post a message for the other peer to pick up
  GET  /relay/recv/<sid>/<id> — relay: long poll to receive a message from the other peer
  GET  /peers                 — debug: see all registered peers
"""

import time
import queue
import threading
from flask import Flask, request, jsonify
from config import RENDEZVOUS_PORT, LONG_POLL_TIMEOUT

app = Flask(__name__)

# Registry: { peer_id -> { lan_ip, lan_port, public_ip, public_port } }
registry = {}

# Waiting long polls: { peer_id -> [threading.Event, ...] }
waiting = {}

# Relay queues: { session_id -> { sender_peer_id -> Queue } }
relay_sessions = {}

registry_lock = threading.Lock()
relay_lock = threading.Lock()


# ── Registration ──────────────────────────────────────────────────────────────

@app.route("/register", methods=["POST"])
def register():
    data = request.json

    peer_id  = data.get("peer_id")
    lan_ip   = data.get("lan_ip")
    lan_port = data.get("lan_port")

    if not peer_id or not lan_ip or not lan_port:
        return jsonify({"error": "peer_id, lan_ip, and lan_port are required"}), 400

    entry = {
        "lan_ip":      lan_ip,
        "lan_port":    int(lan_port),
        "public_ip":   data.get("public_ip"),
        "public_port": int(data["public_port"]) if data.get("public_port") else None,
    }

    with registry_lock:
        registry[peer_id] = entry

        # wake up any long polls waiting for this peer
        if peer_id in waiting:
            for event in waiting[peer_id]:
                event.set()
            del waiting[peer_id]

    print(f"[register] {peer_id} — LAN {lan_ip}:{lan_port} | public {data.get('public_ip')}:{data.get('public_port')}")
    return jsonify({"status": "registered"})


# ── Peer discovery (long poll) ────────────────────────────────────────────────

@app.route("/peer/<peer_id>", methods=["GET"])
def get_peer(peer_id):
    """
    Returns the peer's full info immediately if already registered.
    Otherwise holds the connection open until the peer registers or timeout.
    Client retries automatically on 408.
    """
    with registry_lock:
        if peer_id in registry:
            return jsonify(registry[peer_id])

        event = threading.Event()
        waiting.setdefault(peer_id, []).append(event)

    print(f"[long poll] waiting for '{peer_id}'...")
    triggered = event.wait(timeout=LONG_POLL_TIMEOUT)

    if triggered:
        with registry_lock:
            peer = registry.get(peer_id)
        if peer:
            print(f"[long poll] resolved '{peer_id}'")
            return jsonify(peer)

    return jsonify({"error": "timeout"}), 408


# ── Relay (symmetric NAT fallback) ───────────────────────────────────────────

@app.route("/relay/send/<session_id>/<from_id>", methods=["POST"])
def relay_send(session_id, from_id):
    """
    Peer posts raw message bytes here.
    Queued under from_id so the other peer can pick it up.
    """
    data = request.data
    if not data:
        return jsonify({"error": "empty body"}), 400

    with relay_lock:
        session = relay_sessions.setdefault(session_id, {})
        session.setdefault(from_id, queue.Queue()).put(data)

    return jsonify({"status": "queued"})


@app.route("/relay/recv/<session_id>/<my_id>", methods=["GET"])
def relay_recv(session_id, my_id):
    """
    Long poll: waits for a message sent by the OTHER peer in this session.
    Returns raw bytes when a message is available, 408 on timeout.
    """
    with relay_lock:
        session = relay_sessions.setdefault(session_id, {})
        # find queues belonging to the other peer (anyone who is not my_id)
        other_queues = {k: v for k, v in session.items() if k != my_id}

    for _, q in other_queues.items():
        try:
            message = q.get(timeout=LONG_POLL_TIMEOUT)
            return message, 200, {"Content-Type": "application/octet-stream"}
        except queue.Empty:
            pass

    return jsonify({"error": "timeout"}), 408


# ── Debug ─────────────────────────────────────────────────────────────────────

@app.route("/peers", methods=["GET"])
def list_peers():
    with registry_lock:
        return jsonify(registry)


if __name__ == "__main__":
    print(f"Rendezvous server running on port {RENDEZVOUS_PORT}")
    app.run(host="0.0.0.0", port=RENDEZVOUS_PORT, threaded=True)