#!/usr/bin/env python3
"""Synthetic freeze-signature writers.
Mode p2c: unthrottled buffered 2MiB writes + initiate-only sync_file_range
          every 1MiB — byte-for-byte the pack copy's write signature.
Mode p3b: same, plus every 64MiB a WAIT_BEFORE|WRITE|WAIT_AFTER
          sync_file_range on the completed window — bounded outstanding
          writeback, the mitigation preview.
Rotates (unlink+recreate) at 6GiB to respect the box's ~25GB free.
Usage: writer.py <p2c|p3b> <file> <seconds>"""
import ctypes
import os
import sys
import time

mode, path, secs = sys.argv[1], sys.argv[2], float(sys.argv[3])
rate_mbps = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0
libc = ctypes.CDLL("libc.so.6", use_errno=True)
SFR_WAIT_BEFORE, SFR_WRITE, SFR_WAIT_AFTER = 1, 2, 4


def sfr(fd, off, n, flags):
    libc.sync_file_range(fd, ctypes.c_long(off), ctypes.c_long(n), flags)


buf = b"\x5a" * (2 << 20)
ROTATE = 6 << 30
WINDOW = 64 << 20
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
pos = last_initiate = last_window = 0
total = 0
t_end = time.time() + secs
t_start = time.time()
while time.time() < t_end:
    os.write(fd, buf)
    pos += len(buf)
    total += len(buf)
    while pos - last_initiate >= (1 << 20):
        sfr(fd, last_initiate, 1 << 20, SFR_WRITE)
        last_initiate += 1 << 20
    if mode == "p3b":
        while pos - last_window >= WINDOW:
            sfr(fd, last_window, WINDOW, SFR_WAIT_BEFORE | SFR_WRITE | SFR_WAIT_AFTER)
            last_window += WINDOW
    if rate_mbps > 0:
        expected = total / (rate_mbps * 1e6)
        ahead = expected - (time.time() - t_start)
        if ahead > 0:
            time.sleep(ahead)
    if pos >= ROTATE:
        os.close(fd)
        os.unlink(path)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        pos = last_initiate = last_window = 0
elapsed = time.time() - t_start
print(f"writer {mode}: {total / elapsed / 1e6:.0f} MB/s achieved", file=sys.stderr)
os.close(fd)
try:
    os.unlink(path)
except FileNotFoundError:
    pass
