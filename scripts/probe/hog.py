#!/usr/bin/env python3
"""Memory-bandwidth hog: N worker processes each streaming copies over a
fixed 170MB buffer (allocated once — no page-cache or reclaim involvement).
Usage: hog.py <seconds> [workers=6]"""
import multiprocessing
import sys
import time


def worker(secs: float, duty: float) -> None:
    b = bytearray(170 * 1024 * 1024)
    v = memoryview(b)
    step = 1 << 20
    t_end = time.time() + secs
    while time.time() < t_end:
        t0 = time.time()
        for off in range(0, len(b) - 2 * step, step):
            v[off:off + step] = v[off + step:off + 2 * step]
        if duty < 1.0:
            burst = time.time() - t0
            time.sleep(burst * (1.0 - duty) / duty)


if __name__ == "__main__":
    secs = float(sys.argv[1])
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    duty = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0
    procs = [multiprocessing.Process(target=worker, args=(secs, duty)) for _ in range(n)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
