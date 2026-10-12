
import argparse
import concurrent.futures
import threading
import time
import statistics
from collections import Counter
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

lock = threading.Lock()
latencies = []
statuses = Counter()
total_requests = 0
stop_event = threading.Event()


def check_url(url):
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError("Chỉ chấp nhận URL HTTPS hợp lệ.")


def worker(url, timeout, max_requests):
    global total_requests

    for _ in range(max_requests):
        if stop_event.is_set():
            return

        start = time.perf_counter()
        status = "ERROR"

        try:
            req = Request(
                url,
                headers={"User-Agent": "AuthorizedLoadTest/1.0"}
            )
            with urlopen(req, timeout=timeout) as response:
                status = str(response.status)
                response.read(4096)

        except HTTPError as exc:
            status = str(exc.code)
        except (URLError, TimeoutError, OSError):
            status = "ERROR"

        elapsed = time.perf_counter() - start

        with lock:
            total_requests += 1
            statuses[status] += 1
            latencies.append(elapsed)

            # Dừng nếu xuất hiện nhiều lỗi liên tiếp về mặt tổng thể.
            if statuses["ERROR"] >= 5:
                stop_event.set()


def main():
    parser = argparse.ArgumentParser(
        description="Công cụ kiểm thử tải website có giới hạn"
    )
    parser.add_argument("url", help="URL HTTPS của website bạn sở hữu")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--requests", type=int, default=10,
                        help="Số yêu cầu tối đa cho mỗi luồng")
    parser.add_argument("--timeout", type=float, default=5)
    args = parser.parse_args()

    check_url(args.url)

    if not 1 <= args.workers <= 5:
        raise ValueError("workers phải nằm trong khoảng 1–5.")
    if not 1 <= args.requests <= 20:
        raise ValueError("requests phải nằm trong khoảng 1–20.")
    if not 1 <= args.timeout <= 30:
        raise ValueError("timeout phải nằm trong khoảng 1–30 giây.")

    print("Bắt đầu kiểm thử tải có giới hạn...")
    start = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.workers
    ) as pool:
        futures = [
            pool.submit(worker, args.url, args.timeout, args.requests)
            for _ in range(args.workers)
        ]
        for future in futures:
            future.result()

    duration = time.perf_counter() - start

    print("\n===== BÁO CÁO =====")
    print("Tổng yêu cầu:", total_requests)
    print("Thời gian:", round(duration, 2), "giây")
    print("Trạng thái HTTP:", dict(statuses))

    if latencies:
        print("Độ trễ trung bình:",
              round(statistics.mean(latencies) * 1000), "ms")
        print("Độ trễ cao nhất:",
              round(max(latencies) * 1000), "ms")
        print("Thông lượng:",
              round(total_requests / duration, 2), "yêu cầu/giây")

    if stop_event.is_set():
        print("Đã dừng sớm do xuất hiện nhiều lỗi kết nối.")


if __name__ == "__main__":
    main()