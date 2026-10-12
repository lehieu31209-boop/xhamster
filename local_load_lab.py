#!/usr/bin/env python3
"""
Local Load Lab — safe, bounded traffic simulation for a local test server.

This module intentionally accepts only loopback targets (localhost / 127.0.0.1 / ::1).
It is for learning how a test application behaves under a small, bounded workload.
It is not a DoS/flood tool and cannot target public websites.
"""

import argparse
import concurrent.futures
import ipaddress
import time
import urllib.error
import urllib.request
from collections import Counter
from urllib.parse import urlparse


def validate_local_url(raw: str) -> str:
    if "://" not in raw:
        raw = "http://" + raw
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("Provide a valid http://localhost:PORT URL.")
    host = parsed.hostname.lower()
    is_loopback = host == "localhost"
    try:
        is_loopback = is_loopback or ipaddress.ip_address(host).is_loopback
    except ValueError:
        pass
    if not is_loopback:
        raise ValueError("Safety restriction: target must be localhost / a loopback IP.")
    if parsed.username or parsed.password:
        raise ValueError("Embedded credentials are not accepted.")
    return raw


def one_request(url: str, timeout: float) -> tuple[int | None, float, str | None]:
    start = time.perf_counter()
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "LocalLoadLab/1.0 (local defensive testing)"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            response.read(1024)  # Read at most 1 KiB.
            return response.status, time.perf_counter() - start, None
    except urllib.error.HTTPError as exc:
        return exc.code, time.perf_counter() - start, f"HTTP {exc.code}"
    except Exception as exc:
        return None, time.perf_counter() - start, type(exc).__name__


def run_test(url: str, requests: int, concurrency: int, timeout: float) -> None:
    # Hard bounds to prevent runaway use, even on localhost.
    requests = min(max(requests, 1), 100)
    concurrency = min(max(concurrency, 1), 5)
    timeout = min(max(timeout, 1.0), 5.0)

    print(f"Target: {url}")
    print(f"Requests: {requests}; concurrent workers: {concurrency}; timeout: {timeout}s")
    print("This test is bounded and restricted to loopback targets.")
    input("Press Enter to start, or Ctrl+C to cancel... ")

    results = []
    start = time.perf_counter()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(one_request, url, timeout) for _ in range(requests)]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
    except KeyboardInterrupt:
        print("\nStopped by user.")

    elapsed = time.perf_counter() - start
    statuses = Counter(str(status) if status is not None else "connection-error" for status, _, _ in results)
    durations = sorted(duration for _, duration, _ in results)
    errors = Counter(error for _, _, error in results if error)

    print("\n=== Local Load Lab Report ===")
    print(f"Completed requests: {len(results)} / {requests}")
    print(f"Elapsed: {elapsed:.2f}s")
    if elapsed > 0:
        print(f"Observed throughput: {len(results) / elapsed:.2f} requests/s")
    print(f"HTTP/status counts: {dict(statuses)}")
    if durations:
        print(f"Mean response time: {sum(durations) / len(durations):.3f}s")
        print(f"Max response time: {max(durations):.3f}s")
        print(f"Approx. p95 response time: {durations[min(len(durations)-1, int(0.95 * len(durations)))]:.3f}s")
    if errors:
        print(f"Errors: {dict(errors)}")
    print("\nUse server logs and CPU/RAM metrics to correlate this small test with server behavior.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bounded load simulation for localhost only.")
    parser.add_argument("url", help="Local URL, e.g. http://127.0.0.1:8000/")
    parser.add_argument("--requests", type=int, default=20, help="Request count (1-100; default 20)")
    parser.add_argument("--concurrency", type=int, default=2, help="Workers (1-5; default 2)")
    parser.add_argument("--timeout", type=float, default=3.0, help="Per-request timeout in seconds (max 5)")
    args = parser.parse_args()
    try:
        url = validate_local_url(args.url)
    except ValueError as exc:
        parser.error(str(exc))
    run_test(url, args.requests, args.concurrency, args.timeout)


if __name__ == "__main__":
    main()
