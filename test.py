#!/usr/bin/env python3
"""
Load Test CLI - kiểm thử tải an toàn cho website BẠN SỞ HỮU.

Đặc điểm an toàn:
- Bắt buộc --i-own-this xác nhận quyền sở hữu.
- Bắt buộc scope domain khớp URL (không cho phép quét domain khác).
- Concurrency tối đa 50 (cứng).
- Tổng request tối đa 100000 (cứng).
- Có rate limit mỗi worker (mặc định 5 req/s/worker).
- Có graceful shutdown khi Ctrl+C, in báo cáo dở dang.
- Có threshold tự dừng nếu error rate vượt ngưỡng.

Không có chức năng DoS/flood: mọi request đều đi qua rate limiter,
không có khả năng gửi bão request không giới hạn.
"""

import argparse
import json
import signal
import statistics
import sys
import threading
import time
import urllib.parse
from collections import deque
from datetime import datetime, timezone

import requests


HARD_MAX_CONCURRENCY = 50
HARD_MAX_REQUESTS = 100_000
DEFAULT_TIMEOUT = 10.0


class Stats:
    """Thu thập số liệu thread-safe."""
    def __init__(self):
        self.lock = threading.Lock()
        self.total = 0
        self.success = 0
        self.failed = 0
        self.status_counts = {}
        self.latencies = deque(maxlen=50_000)  # giới hạn bộ nhớ
        self.start_time = None
        self.end_time = None
        self.stopped_early = False
        self.stop_reason = None

    def record(self, status_code, elapsed, ok):
        with self.lock:
            self.total += 1
            if ok:
                self.success += 1
            else:
                self.failed += 1
            key = status_code if status_code else "ERR"
            self.status_counts[key] = self.status_counts.get(key, 0) + 1
            self.latencies.append(elapsed)

    def snapshot(self):
        with self.lock:
            return {
                "total": self.total,
                "success": self.success,
                "failed": self.failed,
                "status_counts": dict(self.status_counts),
                "latencies": list(self.latencies),
            }


class RateLimiter:
    """Giới hạn tần suất request của một worker."""
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.1)
        self.last = 0.0

    def wait(self, stop_event: threading.Event) -> bool:
        now = time.monotonic()
        elapsed = now - self.last
        if elapsed < self.min_interval:
            sleep_for = self.min_interval - elapsed
            # Cho phép thoát nhanh khi có tín hiệu dừng
            if stop_event.wait(timeout=sleep_for):
                return False
        self.last = time.monotonic()
        return True


def normalize_url(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url.rstrip("/")


def get_host(url: str) -> str:
    host = urllib.parse.urlparse(url).hostname
    if not host:
        raise ValueError("URL không hợp lệ.")
    return host


def in_scope(url: str, allowed_hosts) -> bool:
    host = get_host(url).lower()
    for allowed in allowed_hosts:
        allowed = allowed.lower().strip()
        if host == allowed or host.endswith("." + allowed):
            return True
    return False


def worker(worker_id, session, url, limiter, stats, stop_event,
           max_error_rate, timeout):
    """Một worker: gửi request tuần tự, có rate limit, có thể dừng."""
    while not stop_event.is_set():
        if not limiter.wait(stop_event):
            return

        start = time.monotonic()
        try:
            resp = session.get(url, timeout=timeout, allow_redirects=True)
            elapsed = time.monotonic() - start
            ok = 200 <= resp.status_code < 400
            stats.record(resp.status_code, elapsed, ok)
        except requests.RequestException:
            elapsed = time.monotonic() - start
            stats.record(None, elapsed, False)

        # Kiểm tra error rate để tự dừng
        snap = stats.snapshot()
        if snap["total"] >= 20 and max_error_rate is not None:
            rate = snap["failed"] / snap["total"]
            if rate > max_error_rate:
                stop_event.set()
                stats.stopped_early = True
                stats.stop_reason = (
                    f"Error rate {rate:.2%} vượt ngưỡng {max_error_rate:.2%}"
                )
                return


def percentile(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    k = max(0, min(len(values) - 1, int(round((p / 100.0) * (len(values) - 1)))))
    return values[k]


def build_report(url, stats, duration, options):
    snap = stats.snapshot()
    lat = snap["latencies"]
    total = snap["total"]
    report = {
        "target": url,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(duration, 2),
        "options": options,
        "summary": {
            "total_requests": total,
            "success": snap["success"],
            "failed": snap["failed"],
            "error_rate": round(snap["failed"] / total, 4) if total else 0.0,
            "rps": round(total / duration, 2) if duration > 0 else 0.0,
            "status_counts": snap["status_counts"],
        },
        "latency_ms": {
            "min": round(min(lat) * 1000, 2) if lat else 0.0,
            "max": round(max(lat) * 1000, 2) if lat else 0.0,
            "mean": round(statistics.mean(lat) * 1000, 2) if lat else 0.0,
            "p50": round(percentile(lat, 50) * 1000, 2) if lat else 0.0,
            "p90": round(percentile(lat, 90) * 1000, 2) if lat else 0.0,
            "p95": round(percentile(lat, 95) * 1000, 2) if lat else 0.0,
            "p99": round(percentile(lat, 99) * 1000, 2) if lat else 0.0,
        },
    }
    if stats.stopped_early:
        report["stopped_early"] = True
        report["stop_reason"] = stats.stop_reason
    return report


def print_summary(report):
    s = report["summary"]
    lat = report["latency_ms"]
    print("\n" + "=" * 60)
    print(f"Target       : {report['target']}")
    print(f"Duration     : {report['duration_seconds']}s")
    print(f"Total reqs   : {s['total_requests']}")
    print(f"Success      : {s['success']}")
    print(f"Failed       : {s['failed']}  (error rate: {s['error_rate']:.2%})")
    print(f"RPS          : {s['rps']}")
    print(f"Status count : {s['status_counts']}")
    print("-" * 60)
    print(f"Latency (ms) : min={lat['min']}  mean={lat['mean']}  "
          f"p50={lat['p50']}  p90={lat['p90']}  p95={lat['p95']}  "
          f"p99={lat['p99']}  max={lat['max']}")
    if report.get("stopped_early"):
        print(f"!! Dừng sớm: {report['stop_reason']}")
    print("=" * 60)


def main():
    p = argparse.ArgumentParser(
        description="Load Test CLI an toàn cho website bạn sở hữu."
    )
    p.add_argument("--url", required=True,
                   help="URL mục tiêu (http/https).")
    p.add_argument("--i-own-this", action="store_true",
                   help="Bắt buộc: xác nhận bạn sở hữu / được phép test.")
    p.add_argument("--allow-domain", action="append", default=[],
                   help="Domain được phép (có thể lặp). Mặc định suy ra từ URL.")
    p.add_argument("--requests", type=int, default=200,
                   help=f"Tổng số request tối đa (mặc định 200, cứng {HARD_MAX_REQUESTS}).")
    p.add_argument("--concurrency", type=int, default=5,
                   help=f"Số worker song song (mặc định 5, cứng {HARD_MAX_CONCURRENCY}).")
    p.add_argument("--rps-per-worker", type=float, default=5.0,
                   help="Request/giây mỗi worker (mặc định 5).")
    p.add_argument("--max-error-rate", type=float, default=0.2,
                   help="Tự dừng nếu error rate vượt ngưỡng (mặc định 0.2 = 20%%).")
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                   help="Timeout mỗi request (giây), mặc định 10.")
    p.add_argument("--output", default="loadtest_report.json",
                   help="File JSON báo cáo.")
    args = p.parse_args()

    if not args.i_own_this:
        print("Từ chối: bạn phải xác nhận --i-own-this.", file=sys.stderr)
        sys.exit(1)

    target = normalize_url(args.url)
    target_host = get_host(target)
    allowed = args.allow_domain or [target_host]

    if not in_scope(target, allowed):
        print(f"Từ chối: {target_host} không nằm trong scope {allowed}.",
              file=sys.stderr)
        sys.exit(1)

    concurrency = max(1, min(args.concurrency, HARD_MAX_CONCURRENCY))
    requests_cap = max(1, min(args.requests, HARD_MAX_REQUESTS))

    if args.concurrency > HARD_MAX_CONCURRENCY:
        print(f"[!] Concurrency yêu cầu vượt giới hạn, hạ xuống "
              f"{HARD_MAX_CONCURRENCY}.")
    if args.requests > HARD_MAX_REQUESTS:
        print(f"[!] Requests yêu cầu vượt giới hạn, hạ xuống "
              f"{HARD_MAX_REQUESTS}.")

    print(f"[*] Target       : {target}")
    print(f"[*] Scope        : {allowed}")
    print(f"[*] Concurrency  : {concurrency}")
    print(f"[*] Tổng request : {requests_cap}")
    print(f"[*] RPS/worker   : {args.rps_per_worker}")
    print(f"[*] Max err rate : {args.max_error_rate:.2%}")
    print(f"[*] Timeout      : {args.timeout}s")
    print("[*] Nhấn Ctrl+C để dừng và in báo cáo.\n")

    stats = Stats()
    stop_event = threading.Event()

    def on_sigint(signum, frame):
        print("\n[!] Nhận Ctrl+C, đang dừng...")
        stop_event.set()

    signal.signal(signal.SIGINT, on_sigint)

    stats.start_time = time.monotonic()
    start = stats.start_time

    def worker_loop(wid):
        session = requests.Session()
        session.headers.update({
            "User-Agent": "SafeLoadTest/1.0 (+authorized-load-testing)"
        })
        limiter = RateLimiter(args.rps_per_worker)
        while not stop_event.is_set():
            # Kiểm tra tổng request đã đủ chưa
            if stats.snapshot()["total"] >= requests_cap:
                stop_event.set()
                return
            if not limiter.wait(stop_event):
                return

            req_start = time.monotonic()
            try:
                resp = session.get(target, timeout=args.timeout,
                                   allow_redirects=True)
                elapsed = time.monotonic() - req_start
                ok = 200 <= resp.status_code < 400
                stats.record(resp.status_code, elapsed, ok)
            except requests.RequestException:
                elapsed = time.monotonic() - req_start
                stats.record(None, elapsed, False)

            snap = stats.snapshot()
            if (args.max_error_rate is not None
                    and snap["total"] >= 20):
                rate = snap["failed"] / snap["total"]
                if rate > args.max_error_rate:
                    stop_event.set()
                    stats.stopped_early = True
                    stats.stop_reason = (
                        f"Error rate {rate:.2%} vượt ngưỡng "
                        f"{args.max_error_rate:.2%}"
                    )
                    return

    threads = [
        threading.Thread(target=worker_loop, args=(i,), daemon=True)
        for i in range(concurrency)
    ]
    for t in threads:
        t.start()

    # Vòng lặp theo dõi tiến độ
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
            snap = stats.snapshot()
            if snap["total"] >= requests_cap:
                stop_event.set()
                break
            if snap["total"] % 100 == 0 and snap["total"] > 0:
                pass  # tránh spam log
    except KeyboardInterrupt:
        stop_event.set()

    stop_event.set()
    for t in threads:
        t.join(timeout=2)

    stats.end_time = time.monotonic()
    duration = stats.end_time - start

    report = build_report(target, stats, duration, {
        "concurrency": concurrency,
        "requests_cap": requests_cap,
        "rps_per_worker": args.rps_per_worker,
        "max_error_rate": args.max_error_rate,
        "timeout": args.timeout,
    })

    print_summary(report)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"[*] Báo cáo đã ghi: {args.output}")

    # Exit code: 1 nếu có lỗi vượt ngưỡng
    if report["summary"]["error_rate"] > args.max_error_rate:
        sys.exit(2)


if __name__ == "__main__":
    main()
