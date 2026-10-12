
import argparse
import csv
import time
import threading
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

lock = threading.Lock()
stop_event = threading.Event()
stats = {"total": 0, "ok": 0, "errors": 0}
report_file = None


def log_result(writer, url, status, latency, detail):
    now = datetime.now(timezone.utc).isoformat()
    with lock:
        writer.writerow([
            now, url, status, round(latency * 1000, 2), detail
        ])
        report_file.flush()

        stats["total"] += 1
        if status.isdigit() and 200 <= int(status) < 400:
            stats["ok"] += 1
        else:
            stats["errors"] += 1

        print(
            f"[{now}] status={status} "
            f"latency={latency * 1000:.0f}ms "
            f"detail={detail[:160]}"
        )


def worker(url, writer, deadline, timeout, interval):
    while not stop_event.is_set() and time.monotonic() < deadline:
        started = time.monotonic()
        status = "ERROR"
        detail = ""

        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Authorized-Diagnostic/1.0"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as response:
                status = str(response.status)
                response.read(4096)
                detail = "HTTP response received"

        except urllib.error.HTTPError as exc:
            status = str(exc.code)
            detail = str(exc.reason)

        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"

        elapsed = time.monotonic() - started
        log_result(writer, url, status, elapsed, detail)

        remaining = interval - elapsed
        if remaining > 0:
            stop_event.wait(min(remaining, max(
                0, deadline - time.monotonic()
            )))


def main():
    global report_file

    parser = argparse.ArgumentParser(
        description="Website load diagnostic with continuous error logging"
    )
    parser.add_argument("url", help="HTTPS URL you are authorized to test")
    parser.add_argument("--duration", type=int, default=60,
                        help="Duration in seconds; default 60")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=5)
    parser.add_argument("--interval", type=float, default=1.0,
                        help="Minimum delay between requests per worker")
    args = parser.parse_args()

    if not args.url.startswith("https://"):
        parser.error("Only HTTPS URLs are accepted.")
    if not 1 <= args.duration <= 3600:
        parser.error("duration must be between 1 and 3600 seconds.")
    if not 1 <= args.workers <= 5:
        parser.error("workers must be between 1 and 5.")
    if not 1 <= args.timeout <= 30:
        parser.error("timeout must be between 1 and 30 seconds.")
    if args.interval < 0.5:
        parser.error("interval must be at least 0.5 seconds.")

    filename = "web_diagnostic_" + datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    ) + ".csv"

    start = time.monotonic()
    deadline = start + args.duration

    try:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            report_file = f
            writer = csv.writer(f)
            writer.writerow([
                "timestamp_utc", "url", "status",
                "latency_ms", "detail"
            ])

            print(f"Report: {filename}")
            print(f"Duration: {args.duration}s")
            print(f"Workers: {args.workers}")
            print("Errors will be logged; press Ctrl+C to stop.")

            with ThreadPoolExecutor(
                max_workers=args.workers
            ) as pool:
                futures = [
                    pool.submit(
                        worker, args.url, writer, deadline,
                        args.timeout, args.interval
                    )
                    for _ in range(args.workers)
                ]
                try:
                    for future in futures:
                        future.result()
                except KeyboardInterrupt:
                    stop_event.set()
                    print("\nStopping workers and saving report...")
                    for future in futures:
                        try:
                            future.result()
                        except Exception:
                            pass

    except KeyboardInterrupt:
        stop_event.set()

    finally:
        print("\n===== SUMMARY =====")
        print("Requests:", stats["total"])
        print("Successful responses:", stats["ok"])
        print("Errors/non-success responses:", stats["errors"])
        print("CSV report:", filename)


if __name__ == "__main__":
    main()