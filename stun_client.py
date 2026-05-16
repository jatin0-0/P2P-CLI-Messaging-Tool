import socket
import struct
import os
from config import STUN_HOST, STUN_PORT


def get_public_address():
    """
    Sends a STUN Binding Request to Google's STUN server.
    Returns (public_ip, public_port) as seen from the internet.

    STUN message format (RFC 5389):
      - 2 bytes: message type  (0x0001 = Binding Request)
      - 2 bytes: message length (body length, not including the 20-byte header)
      - 4 bytes: magic cookie   (always 0x2112A442)
      - 12 bytes: transaction ID (random, used to match request to response)
    """

    # Build the 20-byte STUN binding request header
    msg_type = 0x0001       # Binding Request
    msg_length = 0x0000     # no body attributes
    magic_cookie = 0x2112A442
    transaction_id = os.urandom(12)  # 12 random bytes

    # Pack into binary: big-endian unsigned short, short, int, then raw bytes
    request = struct.pack(">HHI", msg_type, msg_length, magic_cookie) + transaction_id

    # Create a UDP socket (STUN runs over UDP)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5)

    try:
        sock.sendto(request, (STUN_HOST, STUN_PORT))
        data, _ = sock.recvfrom(2048)
    except socket.timeout:
        raise RuntimeError("STUN request timed out — check your internet connection")
    finally:
        sock.close()

    return parse_stun_response(data)


def parse_stun_response(data):
    """
    Parse the STUN Binding Response to extract the XOR-MAPPED-ADDRESS attribute.
    The server XORs the IP and port with the magic cookie before sending —
    we reverse the XOR to get the real values.
    """

    # Response header is also 20 bytes
    if len(data) < 20:
        raise ValueError("STUN response too short")

    msg_type, msg_length, magic_cookie = struct.unpack(">HHI", data[:8])

    # 0x0101 = Binding Success Response
    if msg_type != 0x0101:
        raise ValueError(f"Unexpected STUN message type: {hex(msg_type)}")

    # Walk through the attributes that follow the 20-byte header
    offset = 20
    while offset < len(data):
        if offset + 4 > len(data):
            break

        attr_type, attr_length = struct.unpack(">HH", data[offset:offset + 4])
        attr_value = data[offset + 4: offset + 4 + attr_length]

        # 0x0020 = XOR-MAPPED-ADDRESS attribute
        if attr_type == 0x0020:
            # attr_value layout:
            #   1 byte: reserved
            #   1 byte: family (0x01 = IPv4)
            #   2 bytes: XOR'd port
            #   4 bytes: XOR'd IP
            family = attr_value[1]
            if family != 0x01:
                raise ValueError("Only IPv4 is supported")

            # Reverse the XOR on port (XOR'd with top 2 bytes of magic cookie)
            xor_port = struct.unpack(">H", attr_value[2:4])[0]
            port = xor_port ^ (magic_cookie >> 16)

            # Reverse the XOR on IP (XOR'd with full magic cookie)
            xor_ip = struct.unpack(">I", attr_value[4:8])[0]
            ip_int = xor_ip ^ magic_cookie
            ip = socket.inet_ntoa(struct.pack(">I", ip_int))

            return ip, port

        # Attributes are padded to 4-byte boundaries
        offset += 4 + attr_length + (4 - attr_length % 4) % 4

    raise ValueError("XOR-MAPPED-ADDRESS not found in STUN response")


if __name__ == "__main__":
    print("Contacting STUN server...")
    ip, port = get_public_address()
    print(f"Your public address: {ip}:{port}")