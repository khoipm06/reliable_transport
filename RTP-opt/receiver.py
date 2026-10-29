import argparse
import select
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

MAX_PACKET = 1472
HEADER_SIZE = 16
TIMEOUT = 0.5  # seconds


def make_pkt(pkt_type, seq_num, payload=b""):
    header = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(payload))
    header.checksum = compute_checksum(header / payload)
    return bytes(header / payload)


def send_ack(s, addr, seq_num):
    s.sendto(make_pkt(3, seq_num), addr)


def receiver(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    expected = None  # next in-order seq_num; None until START arrives
    sender_addr = None
    buffer = {}  # out-of-order buffered DATA: seq -> payload

    def deliver():
        """Flush buffered in-order packets to stdout."""
        nonlocal expected
        while expected in buffer:
            sys.stdout.buffer.write(buffer.pop(expected))
            expected += 1
        sys.stdout.flush()

    def recv_pkt(timeout):
        """Return the next valid packet header+payload, or None on timeout."""
        r, _, _ = select.select([s], [], [], timeout)
        if not r:
            return None
        pkt, addr = s.recvfrom(2048)
        header = PacketHeader(pkt[:16])
        received_checksum = header.checksum
        header.checksum = 0
        payload = pkt[16 : 16 + header.length]
        expected_cs = compute_checksum(header / payload)
        if received_checksum != expected_cs:
            return None
        return header, payload, addr

    while True:
        received = recv_pkt(None)
        if received is None:
            continue
        header, payload, addr = received

        if header.type == 0:  # START
            if expected is None:
                expected = 1
                sender_addr = addr
                send_ack(s, sender_addr, expected)
            continue  # ignore START mid-connection

        if addr != sender_addr:
            continue  # only handle one sender at a time

        if header.type == 2:  # DATA
            seq = header.seq_num
            if seq >= expected + window_size or seq < expected:
                # Outside window (or old duplicate): drop, no ACK.
                continue
            if seq not in buffer:
                buffer[seq] = payload
            if seq == expected:
                deliver()
            # Individual ACK: echo the received seq_num instead of cumulative.
            send_ack(s, sender_addr, seq)

        elif header.type == 1:  # END
            send_ack(s, sender_addr, header.seq_num)
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

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
