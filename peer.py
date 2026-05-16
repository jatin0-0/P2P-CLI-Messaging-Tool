"""
peer.py — full peer client with automatic network path selection.

Usage:
  python peer.py <my_id> <other_peer_id> [local_port]

  local_port defaults to 9000. If two peers run on the same machine,
  give them different ports:
    python peer.py alice bob 9000
    python peer.py bob alice 9001

Connection strategy (fully automatic, no config flags needed):
  1. Both peers register their LAN IP and public IP with the rendezvous server.
  2. After discovering each other's addresses, peers compare LAN subnets:
       Same subnet  → connect directly via LAN IP   (no hole punch needed)
       Different    → attempt UDP hole punch (3s timeout)
                        success → direct P2P via public IP
                        failure → HTTP relay through rendezvous server (symmetric NAT fallback)
"""

import sys
import socket
import threading
import time
import requests
from stun_client import get_public_address
from config import RENDEZVOUS_HOST, RENDEZVOUS_PORT, LONG_POLL_TIMEOUT, LOCAL_UDP_PORT

RENDEZVOUS_URL = f"http://{RENDEZVOUS_HOST}:{RENDEZVOUS_PORT}"


# ── Network helpers ───────────────────────────────────────────────────────────

def get_lan_ip():
    """
    Returns this machine's LAN IP (e.g. 192.168.1.5).
    Trick: connect a UDP socket to an external address — no packet is actually
    sent, but the OS picks the correct outbound interface, revealing the LAN IP.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def same_subnet(ip1, ip2):
    """
    Returns True if both IPs are on the same /24 subnet.
    e.g. 192.168.1.5 and 192.168.1.8  → True
         192.168.1.5 and 103.21.x.x   → False
    Compares the first three octets only.
    """
    return ip1.rsplit(".", 1)[0] == ip2.rsplit(".", 1)[0]


# ── Rendezvous ────────────────────────────────────────────────────────────────

def register_with_rendezvous(peer_id, lan_ip, lan_port, public_ip=None, public_port=None):
    """Register this peer's LAN and public addresses with the rendezvous server."""
    payload = {
        "peer_id":  peer_id,
        "lan_ip":   lan_ip,
        "lan_port": lan_port,
    }
    if public_ip:
        payload["public_ip"]   = public_ip
        payload["public_port"] = public_port

    response = requests.post(f"{RENDEZVOUS_URL}/register", json=payload)
    response.raise_for_status()
    print(f"[rendezvous] Registered '{peer_id}' — LAN {lan_ip}:{lan_port} | public {public_ip}:{public_port}")


def wait_for_peer(other_id):
    """
    Long poll the rendezvous server until the other peer registers.
    Retries automatically on 408 timeout.
    Returns the peer's full info dict: { lan_ip, lan_port, public_ip, public_port }
    """
    print(f"[rendezvous] Waiting for '{other_id}' to come online...")
    while True:
        try:
            response = requests.get(
                f"{RENDEZVOUS_URL}/peer/{other_id}",
                timeout=LONG_POLL_TIMEOUT + 5
            )
            if response.status_code == 200:
                info = response.json()
                print(f"[rendezvous] Found '{other_id}' — LAN {info['lan_ip']}:{info['lan_port']} | public {info.get('public_ip')}:{info.get('public_port')}")
                return info
            elif response.status_code == 408:
                print("[rendezvous] Long poll timed out, retrying...")
                continue
        except requests.exceptions.ConnectionError:
            print("[rendezvous] Cannot reach rendezvous server, retrying in 2s...")
            time.sleep(2)


# ── Direct P2P (UDP) ──────────────────────────────────────────────────────────

def try_hole_punch(sock, peer_ip, peer_port, my_id, timeout=3):
    """
    Attempt UDP hole punch to peer_ip:peer_port.
    Sends 5 probe packets then waits up to `timeout` seconds for any response.
    Returns True if the peer responds (hole punch succeeded), False otherwise.
    """
    print(f"[holepunch] Firing UDP probes at {peer_ip}:{peer_port}...")
    for _ in range(5):
        sock.sendto(f"HELLO:{my_id}".encode(), (peer_ip, peer_port))
        time.sleep(0.2)

    sock.settimeout(timeout)
    try:
        data, _ = sock.recvfrom(4096)
        print("[holepunch] Response received — direct P2P established")
        sock.settimeout(None)
        return True
    except socket.timeout:
        print("[holepunch] No response — hole punch failed (likely symmetric NAT)")
        sock.settimeout(None)
        return False


def receive_loop(sock):
    """
    Background thread for direct UDP chat.
    Reads incoming packets and prints them.
    Ignores HELLO probe packets used during hole punching.
    """
    while True:
        try:
            data, addr = sock.recvfrom(4096)
            message = data.decode(errors="replace")
            if not message.startswith("HELLO:"):
                print(f"\n[peer] {message}")
                print("you> ", end="", flush=True)
        except OSError:
            # socket was closed — exit thread cleanly
            break


def direct_chat_loop(sock, peer_ip, peer_port):
    """Send/receive loop for direct UDP connection (LAN or hole-punched)."""
    print("[p2p] Direct connection active. Start chatting!")
    print("(type a message and press Enter — Ctrl+C to quit)\n")

    recv_thread = threading.Thread(target=receive_loop, args=(sock,), daemon=True)
    recv_thread.start()

    try:
        while True:
            print("you> ", end="", flush=True)
            message = input()
            if message:
                sock.sendto(message.encode(), (peer_ip, peer_port))
    except KeyboardInterrupt:
        print("\n[p2p] Disconnecting.")


# ── Relay fallback (HTTP, symmetric NAT) ─────────────────────────────────────

def relay_recv_loop(session_id, my_id):
    """
    Background thread: long polls the rendezvous server for relayed messages
    and prints them as they arrive.
    """
    while True:
        try:
            r = requests.get(
                f"{RENDEZVOUS_URL}/relay/recv/{session_id}/{my_id}",
                timeout=LONG_POLL_TIMEOUT + 5
            )
            if r.status_code == 200:
                print(f"\n[peer] {r.content.decode(errors='replace')}")
                print("you> ", end="", flush=True)
            # 408 = timeout, just retry
        except Exception:
            time.sleep(1)


def relay_chat_loop(session_id, my_id):
    """Send/receive loop using HTTP relay through the rendezvous server."""
    print("[relay] Symmetric NAT detected — routing through relay server.")
    print("[relay] Messages are forwarded by the rendezvous server.")
    print("(type a message and press Enter — Ctrl+C to quit)\n")

    recv_thread = threading.Thread(
        target=relay_recv_loop, args=(session_id, my_id), daemon=True
    )
    recv_thread.start()

    try:
        while True:
            print("you> ", end="", flush=True)
            message = input()
            if message:
                requests.post(
                    f"{RENDEZVOUS_URL}/relay/send/{session_id}/{my_id}",
                    data=message.encode(),
                    timeout=5
                )
    except KeyboardInterrupt:
        print("\n[relay] Disconnecting.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 3:
        print("Usage: python peer.py <my_id> <other_peer_id> [local_port]")
        sys.exit(1)

    my_id    = sys.argv[1]
    other_id = sys.argv[2]
    udp_port = int(sys.argv[3]) if len(sys.argv) == 4 else LOCAL_UDP_PORT

    # Step 1: bind UDP socket first — STUN must see this exact port
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", udp_port))
    print(f"[socket] Bound UDP on port {udp_port}")

    # Step 2: get LAN IP (always available, no internet needed)
    my_lan_ip = get_lan_ip()
    print(f"[network] LAN IP: {my_lan_ip}")

    # Step 3: try to discover public IP via STUN (may fail on restricted networks)
    public_ip, public_port = None, None
    print("[stun] Discovering public address...")
    try:
        public_ip, public_port = get_public_address()
        print(f"[stun] Public address: {public_ip}:{public_port}")
    except Exception as e:
        print(f"[stun] Failed ({e}) — will use LAN only")

    # Step 4: register both addresses with the rendezvous server
    register_with_rendezvous(my_id, my_lan_ip, udp_port, public_ip, public_port)

    # Step 5: long poll for the other peer's addresses
    peer_info        = wait_for_peer(other_id)
    peer_lan_ip      = peer_info["lan_ip"]
    peer_lan_port    = int(peer_info["lan_port"])
    peer_public_ip   = peer_info.get("public_ip")
    peer_public_port = int(peer_info["public_port"]) if peer_info.get("public_port") else None

    # Step 6: choose connection strategy
    if same_subnet(my_lan_ip, peer_lan_ip):
        # ── Path A: same LAN — connect directly, no hole punch needed ──
        print(f"[network] Same subnet ({my_lan_ip} / {peer_lan_ip}) — direct LAN connection")
        direct_chat_loop(sock, peer_lan_ip, peer_lan_port)

    elif peer_public_ip and public_ip:
        # ── Path B: different networks — try hole punch first ──
        print(f"[network] Different networks — attempting hole punch")
        time.sleep(1)  # give both peers time to reach this point
        success = try_hole_punch(sock, peer_public_ip, peer_public_port, my_id, timeout=3)

        if success:
            direct_chat_loop(sock, peer_public_ip, peer_public_port)
        else:
            # ── Path C: symmetric NAT — fall back to relay ──
            # session_id is the same on both sides: sorted names joined
            session_id = "_".join(sorted([my_id, other_id]))
            relay_chat_loop(session_id, my_id)

    else:
        # No public IP available on one or both sides — go straight to relay
        print("[network] No public IP available — using relay")
        session_id = "_".join(sorted([my_id, other_id]))
        relay_chat_loop(session_id, my_id)

    sock.close()


if __name__ == "__main__":
    main()