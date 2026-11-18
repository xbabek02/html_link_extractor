from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from typing import List, Optional
import urllib.request
import urllib.error

from models import ProducerItem

class Producer:
    def __init__(self, out_queue: Queue, num_threads: int = 4):
        self.out_queue = out_queue
        self.num_threads = max(1, num_threads)

    def _fetch_url(self, url: str) -> dict:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (ProducerConsumer/1.0)"}
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                status_code = response.getcode()
                charset = response.headers.get_content_charset() or "utf-8"
                content = response.read().decode(charset, errors="replace")
                return {
                    "html_code": status_code,
                    "message": "OK",
                    "content": content
                }
        except urllib.error.HTTPError as e:
            return {
                "html_code": e.code,
                "message": f"HTTP Error {e.code}: {e.reason}",
                "content": None
            }
        except urllib.error.URLError as e:
            return {
                "html_code": None,
                "message": f"URL Error: {e.reason}",
                "content": None
            }
        except Exception as e:
            return {
                "html_code": None,
                "message": f"Error: {str(e)}",
                "content": None
            }

    def _process_urls(self, urls: List[str]) -> None:
        cleaned_urls = [u.strip() for u in urls if u.strip()]

        if not cleaned_urls:
            self.out_queue.put(
                ProducerItem(
                    url=None,
                    html_code=None,
                    message="No URLs to process",
                    content=None,
                    finished=True
                )
            )
            return

        total_urls = len(cleaned_urls)

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            results = executor.map(self._fetch_url, cleaned_urls)

            for index, (url, res) in enumerate(zip(cleaned_urls, results)):
                is_last = (index == total_urls - 1)
                item = ProducerItem(
                    url=url,
                    html_code=res["html_code"],
                    message=res["message"],
                    content=res["content"],
                    finished=is_last
                )
                self.out_queue.put(item)

    def process_file(self, filename: str) -> None:
        try:
            with open(filename, "r", encoding="utf-8") as f:
                urls = f.readlines()
            self._process_urls(urls)
        except OSError as e:
            self.out_queue.put(
                ProducerItem(
                    url=None,
                    html_code=None,
                    message=f"File Error: {str(e)}",
                    content=None,
                    finished=True
                )
            )

    def process_string(self, input_str: str) -> None:
        urls = input_str.splitlines()
        self._process_urls(urls)