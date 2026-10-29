# CS 145 Project 4

## Author and collaborators
### Author name
Khoi PM (khoipm06)

### Collaborators
None.

## Report

Both `RTP-base` (cumulative ACK, Go-Back-N style) and `RTP-opt` (individual
ACKs, per-packet retransmission) pass the provided proxy-based test script
with all four error types (`0123`: delay, reorder, drop, jam).

Key implementation points:

- Checksum verification must zero the `checksum` header field before
  recomputing the CRC32 over `header / payload`; comparing against the
  stored value without zeroing fails on every packet.
- The sender uses a monotonic 500ms deadline; any window advance resets it.
  On expiry the base sender retransmits the whole unACKed window, the
  optimized sender retransmits only unACKed in-window packets (single
  timer for the window as required).
- The receiver buffers out-of-order packets, drops `seq >= N + window_size`,
  and only exits after ACKing END. The optimized receiver ACKs by echoing
  the received `seq_num` and silently drops out-of-window duplicates.

## Citations

- Course provided starter code, proxy simulator, and spec (README.md).
- Python `socket`/`select` standard library documentation.

## Grading notes (if any)

None.

## Extra credit attempted (if any)

None.
