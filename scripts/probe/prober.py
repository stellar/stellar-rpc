#!/usr/bin/env python3
"""WAL-shaped fsync prober: 2MiB append + fdatasync every 250ms (~8MB/s, the
hot WAL's rate class). Logs one CSV row per op: wall_s,write_ms,sync_ms.
Self-throttling is visible: achieved rate prints in the 5s summaries on stderr.
Usage: prober.py <file> <seconds> <csv-out>"""
import os
import sys
import time

path, secs, out = sys.argv[1], float(sys.argv[2]), sys.argv[3]
buf = b"\xa5" * (2 << 20)
ROTATE = 1 << 30
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
csv = open(out, "w", buffering=1)
csv.write("wall_s,write_ms,sync_ms\n")
written = 0
t_end = time.time() + secs
last_report, ops = time.time(), 0
while time.time() < t_end:
    t0 = time.time()
    os.write(fd, buf)
    t1 = time.time()
    os.fdatasync(fd)
    t2 = time.time()
    csv.write(f"{t0:.3f},{(t1 - t0) * 1e3:.3f},{(t2 - t1) * 1e3:.3f}\n")
    written += len(buf)
    ops += 1
    if written >= ROTATE:
        os.close(fd)
        os.unlink(path)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        written = 0
    if t2 - last_report >= 5:
        print(f"prober: {ops / (t2 - last_report):.1f} syncs/s", file=sys.stderr)
        last_report, ops = t2, 0
    time.sleep(max(0.0, 0.25 - (time.time() - t0)))
os.close(fd)
try:
    os.unlink(path)
except FileNotFoundError:
    pass
