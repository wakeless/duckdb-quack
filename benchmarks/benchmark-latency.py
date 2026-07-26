#!/usr/bin/env python3
"""Measure quack's round-trip cost over a link with injected latency.

Quack's per-request cost is invisible on loopback, where opening a connection is
free. This script puts a delaying TCP proxy between a quack client and a quack
server so the cost of *opening* a connection can be told apart from the cost of
using one.

The proxy models a link as:

  * `--handshake-rtts` round trips to establish a connection (1 for plain TCP,
    2 for TCP plus a TLS 1.3 handshake, which is what a TLS-terminating load
    balancer in front of the server costs)
  * one round trip per turn of the conversation

It also counts the TCP connections it accepts. That is an out-of-process check
on whether the client reuses connections, independent of anything the client or
server reports about itself: compare `accepted` with `requests` below. A client
that opens a fresh connection per request pays `handshake_rtts + 1` round trips
for every request; one that holds a connection open pays 1.

Needs only the standard library and a built duckdb binary (`make`).

Usage:
    python benchmarks/benchmark-latency.py                      # 250ms RTT
    python benchmarks/benchmark-latency.py --rtt 0 --rtt 50 --rtt 250
    python benchmarks/benchmark-latency.py --handshake-rtts 1   # no TLS
"""

import argparse
import os
import re
import socket
import statistics
import subprocess
import sys
import threading
import time

TOKEN = "benchmarklatency"
RUN_TIME_RE = re.compile(r"^Run Time \(s\): real ([0-9.]+)")
COUNTER_RE = re.compile(r"^COUNTERS\s+(\d+)\s+(\d+)$")

CLIENT_TO_UPSTREAM = "up"
UPSTREAM_TO_CLIENT = "down"


class DelayingProxy:
    """A TCP proxy that adds latency and counts the connections it accepts."""

    def __init__(self, upstream_port, rtt_seconds, handshake_rtts):
        # "localhost" rather than a literal address: the server binds whichever family
        # getaddrinfo puts first, which is ::1 on a dual-stack host.
        self.upstream = ("localhost", upstream_port)
        self.rtt = rtt_seconds
        self.handshake_rtts = handshake_rtts
        self.accepted = 0
        self._lock = threading.Lock()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(128)
        self.port = self._listener.getsockname()[1]
        self._stop = threading.Event()
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def stop(self):
        self._stop.set()
        try:
            self._listener.close()
        except OSError:
            pass

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            with self._lock:
                self.accepted += 1
            threading.Thread(target=self._serve, args=(client,), daemon=True).start()

    def _serve(self, client):
        # Establishing a connection costs its round trips before any payload moves.
        time.sleep(self.rtt * self.handshake_rtts)
        try:
            upstream = socket.create_connection(self.upstream)
        except OSError:
            client.close()
            return
        for sock in (client, upstream):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # A hop is charged per turn of the conversation, not per TCP segment, so a
        # multi-segment body counts as one hop and large results are not over-penalised.
        turn_lock = threading.Lock()
        last_direction = [None]

        def pump(src, dst, direction):
            try:
                while True:
                    data = src.recv(65536)
                    if not data:
                        break
                    with turn_lock:
                        turned = last_direction[0] != direction
                        last_direction[0] = direction
                    if turned:
                        time.sleep(self.rtt / 2)
                    dst.sendall(data)
            except OSError:
                pass
            finally:
                for sock in (src, dst):
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    sock.close()

        for src, dst, direction in (
            (client, upstream, CLIENT_TO_UPSTREAM),
            (upstream, client, UPSTREAM_TO_CLIENT),
        ):
            threading.Thread(target=pump, args=(src, dst, direction), daemon=True).start()


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def build_script(server_port, proxy_port, rows, repeats, extension):
    """Build the SQL script, plus the labels of the statements we time.

    The server and the client share one process: quack speaks HTTP over a socket
    either way, so traffic still crosses the proxy. Every statement after
    `.timer on` prints exactly one `Run Time` line, in order, which is how the
    timings are matched up to labels.
    """
    setup = [
        f"LOAD '{extension}';" if extension else "INSTALL quack FROM core_nightly; LOAD quack;",
        "LOAD httpfs;",
        f"CREATE TABLE t AS SELECT range i FROM range({rows});",
        f"SELECT listen_uri FROM quack_serve('quack:localhost:{server_port}', token='{TOKEN}');",
        f"CREATE SECRET (TYPE quack, token '{TOKEN}');",
        # one client thread keeps the connection count deterministic
        "PRAGMA threads=1;",
        ".timer on",
    ]

    uri = f"quack:localhost:{proxy_port}"
    timed = [("attach", f"ATTACH '{uri}' AS remote;")]
    # The first run of each probe pays for opening a connection; the medians below
    # discard it, so what is reported is the steady-state cost.
    for i in range(repeats + 1):
        timed.append((f"count/{i}", "SELECT count(*) FROM remote.main.t;"))
    for i in range(repeats + 1):
        timed.append((f"query/{i}", f"SELECT * FROM quack_query('{uri}', 'SELECT 42');"))

    teardown = [
        ".timer off",
        ".mode list",
        ".headers off",
        "SELECT 'COUNTERS', info['client_connections'], info['client_requests'] FROM quack_server_list();",
        "DETACH remote;",
    ]

    script = "\n".join(setup + [sql for _, sql in timed] + teardown) + "\n"
    return script, [label for label, _ in timed]


def parse_output(output, labels):
    times = [float(m.group(1)) for m in (RUN_TIME_RE.match(l) for l in output.splitlines()) if m]
    if len(times) < len(labels):
        raise RuntimeError(
            f"expected {len(labels)} timed statements, got {len(times)}\n--- output ---\n{output}"
        )
    measured = dict(zip(labels, times))

    counters = (0, 0)
    for line in output.splitlines():
        parts = line.split("|") if "|" in line else line.split()
        if parts and parts[0].strip("'\" ") == "COUNTERS" and len(parts) >= 3:
            counters = (int(parts[1]), int(parts[2]))
    return measured, counters


def median_of(measured, prefix):
    values = [v for k, v in measured.items() if k.startswith(prefix + "/") and not k.endswith("/0")]
    return statistics.median(values) if values else float("nan")


def run(duckdb_bin, rtt_ms, handshake_rtts, repeats, rows, extension):
    rtt = rtt_ms / 1000.0
    server_port = free_port()
    proxy = DelayingProxy(server_port, rtt, handshake_rtts)
    try:
        script, labels = build_script(server_port, proxy.port, rows, repeats, extension)
        proc = subprocess.run(
            [duckdb_bin, "-unsigned", "-batch", "-init", "/dev/null"],
            input=script,
            capture_output=True,
            text=True,
            timeout=600,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"duckdb exited {proc.returncode}\n{proc.stdout}\n{proc.stderr}")
        for marker in ("Error:", "error:"):
            if marker in proc.stdout:
                raise RuntimeError(f"duckdb reported an error\n{proc.stdout}")
        measured, (server_connections, requests) = parse_output(proc.stdout, labels)
    finally:
        proxy.stop()

    return {
        "rtt_ms": rtt_ms,
        "attach_s": measured["attach"],
        "count_s": median_of(measured, "count"),
        "query_s": median_of(measured, "query"),
        "accepted": proxy.accepted,
        "requests": requests,
        "server_connections": server_connections,
    }


def main():
    default_extension = os.path.join("build", "release", "extension", "quack", "quack.duckdb_extension")
    default_duckdb = os.path.join("build", "release", "duckdb")

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--rtt",
        type=float,
        action="append",
        metavar="MS",
        help="round-trip time to inject, in milliseconds (repeatable; default 250)",
    )
    parser.add_argument(
        "--handshake-rtts",
        type=float,
        default=2.0,
        help="round trips needed to open a connection: 1 for plain TCP, 2 with TLS (default 2)",
    )
    parser.add_argument("--repeats", type=int, default=5, help="timed runs per probe (default 5)")
    parser.add_argument("--rows", type=int, default=10_000, help="rows in the remote table (default 10000)")
    parser.add_argument("--duckdb", default=default_duckdb, help=f"duckdb binary (default {default_duckdb})")
    parser.add_argument(
        "--extension",
        default=default_extension if os.path.exists(default_extension) else None,
        help="path to quack.duckdb_extension (defaults to the local release build, else core_nightly)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.duckdb):
        parser.error(f"{args.duckdb} not found - run `make` first, or pass --duckdb")

    rtts = args.rtt if args.rtt else [250.0]

    print(f"quack latency benchmark: handshake = {args.handshake_rtts:g} RTT, {args.repeats} repeats")
    print(f"extension: {args.extension or 'core_nightly'}\n")
    header = f"{'RTT':>8}  {'ATTACH':>9}  {'count(*)':>9}  {'quack_query':>11}  {'accepted':>8}  {'requests':>8}"
    print(header)
    print("-" * len(header))

    rows = []
    for rtt_ms in rtts:
        r = run(args.duckdb, rtt_ms, args.handshake_rtts, args.repeats, args.rows, args.extension)
        rows.append(r)
        print(
            f"{r['rtt_ms']:>6.0f}ms  {r['attach_s']:>8.3f}s  {r['count_s']:>8.3f}s  "
            f"{r['query_s']:>10.3f}s  {r['accepted']:>8}  {r['requests']:>8}"
        )

    print()
    for r in rows:
        if r["rtt_ms"] <= 0:
            continue
        rtt = r["rtt_ms"] / 1000.0
        print(
            f"at {r['rtt_ms']:.0f}ms RTT: ATTACH cost {r['attach_s'] / rtt:.1f} RTT, "
            f"count(*) cost {r['count_s'] / rtt:.1f} RTT, "
            f"quack_query cost {r['query_s'] / rtt:.1f} RTT"
        )

    worst = max(rows, key=lambda r: r["requests"])
    if worst["accepted"] >= worst["requests"]:
        print(
            f"\nno connection reuse: {worst['accepted']} connections for {worst['requests']} requests - "
            f"every request pays {args.handshake_rtts:g} extra RTT to open one"
        )
    else:
        print(f"\nconnections are reused: {worst['accepted']} connections served {worst['requests']} requests")
    return 0


if __name__ == "__main__":
    sys.exit(main())
