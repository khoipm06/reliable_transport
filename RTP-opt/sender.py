import argparse
import select
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

MAX_PACKET = 1472
HEADER_SIZE = 16
PAYLOAD_SIZE = MAX_PACKET - HEADER_SIZE
TIMEOUT = 0.5  # seconds


def make_pkt(pkt_type, seq_num, payload=b""):
    header = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(payload))
    header.checksum = compute_checksum(header / payload)
    return bytes(header / payload)


def sender(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    addr = (receiver_ip, receiver_port)
    message = sys.stdin.buffer.read()
    packets = [message[i : i + PAYLOAD_SIZE] for i in range(0, len(message), PAYLOAD_SIZE)]
    final_ack = len(packets) + 1

    start_pkt = make_pkt(0, 0)
    end_pkt = make_pkt(1, final_ack)
    data_pkts = {seq: make_pkt(2, seq, data) for seq, data in enumerate(packets, start=1)}

    def recv_ack(timeout):
        """Wait for the next valid ACK; return its seq_num or None on timeout."""
        r, _, _ = select.select([s], [], [], timeout)
        while r:
            pkt, _ = s.recvfrom(2048)
            header = PacketHeader(pkt[:16])
            received_checksum = header.checksum
            header.checksum = 0
            payload = pkt[16 : 16 + header.length]
            expected = compute_checksum(header / payload)
            if received_checksum != expected or header.type != 3:
                r, _, _ = select.select([s], [], [], timeout)
                continue
            return header.seq_num
        return None

    # Handshake: retransmit START every 500ms until ACK-START (seq 1) arrives.
    s.sendto(start_pkt, addr)
    while recv_ack(TIMEOUT) != 1:
        s.sendto(start_pkt, addr)

    window_base = 1  # lowest seq in the current window
    next_seq = 1  # next seq to send
    final_seq = len(packets)
    acked = set()  # individually acknowledged seq nums in the current window

    def send_new():
        nonlocal next_seq
        while next_seq < window_base + window_size and next_seq <= final_seq:
            s.sendto(data_pkts[next_seq], addr)
            next_seq += 1

    def slide():
        """Move window base past every leading ACKed packet."""
        nonlocal window_base, acked
        while window_base in acked:
            acked.discard(window_base)
            window_base += 1

    send_new()
    deadline = time.monotonic() + TIMEOUT
    while window_base <= final_seq:
        remaining = deadline - time.monotonic()
        ack = recv_ack(remaining if remaining > 0 else 0)
        if ack is None:
            # Single window timer expired: retransmit only unACKed packets.
            for seq in range(window_base, next_seq):
                if seq not in acked:
                    s.sendto(data_pkts[seq], addr)
            deadline = time.monotonic() + TIMEOUT
            continue
        if window_base <= ack <= final_seq:
            acked.add(ack)
            before = window_base
            slide()
            send_new()
            if window_base != before:
                deadline = time.monotonic() + TIMEOUT

    # Teardown: send END; exit on ACK-END or once 500ms has passed since sending it.
    s.sendto(end_pkt, addr)
    deadline = time.monotonic() + TIMEOUT
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        ack = recv_ack(remaining)
        if ack == final_ack + 1:
            break
    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which the receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
