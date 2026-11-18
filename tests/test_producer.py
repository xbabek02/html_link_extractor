from email.message import Message
from pathlib import Path
from queue import Queue
import sys
import time
import unittest
from unittest.mock import MagicMock, mock_open, patch
import urllib.error
import threading

# Ensure src/ is on sys.path so modules inside src resolve their peer imports
src_path = str(Path(__file__).resolve().parent.parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from producer import Producer, ProducerItem


class TestProducerInitialization(unittest.TestCase):
    """Verifies producer setup and thread clamp parameters."""

    def test_init_parameters(self):
        q = Queue()
        producer = Producer(out_queue=q, num_threads=8)
        self.assertIs(producer.out_queue, q)
        self.assertEqual(producer.num_threads, 8)

    def test_init_num_threads_minimum_clamp(self):
        q = Queue()
        producer = Producer(out_queue=q, num_threads=0)
        self.assertEqual(producer.num_threads, 1)


class TestProducerItemStructureAndOrder(unittest.TestCase):
    """Verifies that the producer preserves input ordering and flags."""

    def setUp(self):
        self.queue = Queue()
        self.producer = Producer(out_queue=self.queue, num_threads=4)

    def _drain_queue(self):
        items = []
        while not self.queue.empty():
            items.append(self.queue.get_nowait())
        return items

    @patch.object(Producer, "_fetch_url")
    def test_order_preserved_when_requests_finish_out_of_order(self, mock_fetch):
        """Ensures that even if URL 2 finishes before URL 1, URL 1 is enqueued first."""
        def simulate_variable_latency(url):
            if "slow" in url:
                time.sleep(0.05)
                return {"html_code": 200, "message": "OK", "content": "slow site"}
            return {"html_code": 200, "message": "OK", "content": "fast site"}

        mock_fetch.side_effect = simulate_variable_latency

        self.producer.process_string("http://slow.com\nhttp://fast.com")
        items = self._drain_queue()

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].content, "slow site")
        self.assertFalse(items[0].finished)

        self.assertEqual(items[1].content, "fast site")
        self.assertTrue(items[1].finished)

    @patch.object(Producer, "_fetch_url")
    def test_finished_flag_multi_line(self, mock_fetch):
        """Verifies only the final item receives finished=True."""
        mock_fetch.return_value = {"html_code": 200, "message": "OK", "content": "data"}

        urls = "\n".join([f"http://example{i}.com" for i in range(4)])
        self.producer.process_string(urls)
        items = self._drain_queue()

        self.assertEqual(len(items), 4)
        for i in range(3):
            self.assertFalse(items[i].finished, f"Item {i} should have finished=False")
        self.assertTrue(items[3].finished, "Final item must have finished=True")

    @patch.object(Producer, "_fetch_url")
    def test_finished_flag_single_url(self, mock_fetch):
        """Verifies single URL input produces finished=True on its first and only item."""
        mock_fetch.return_value = {"html_code": 200, "message": "OK", "content": "data"}

        self.producer.process_string("http://only-one.com")
        items = self._drain_queue()

        self.assertEqual(len(items), 1)
        self.assertTrue(items[0].finished)
        self.assertEqual(items[0].html_code, 200)

    @patch.object(Producer, "_fetch_url")
    def test_empty_and_whitespace_input(self, mock_fetch):
        """Verifies empty or whitespace-only inputs yield a terminal sentinel item."""
        self.producer.process_string("   \n\n\t\n   ")
        items = self._drain_queue()

        self.assertEqual(len(items), 1)
        self.assertTrue(items[0].finished)
        self.assertIsNone(items[0].html_code)
        self.assertEqual(items[0].message, "No URLs to process")
        self.assertIsNone(items[0].content)
        mock_fetch.assert_not_called()


class TestProducerFileProcessing(unittest.TestCase):
    """Verifies file reading and disk I/O error handling."""

    def setUp(self):
        self.queue = Queue()
        self.producer = Producer(out_queue=self.queue, num_threads=2)

    def _drain_queue(self):
        items = []
        while not self.queue.empty():
            items.append(self.queue.get_nowait())
        return items

    @patch.object(Producer, "_fetch_url")
    def test_process_file_success(self, mock_fetch):
        file_data = "https://a.org\nhttps://b.org\n"
        mock_fetch.side_effect = [
            {"html_code": 200, "message": "OK", "content": "page A"},
            {"html_code": 200, "message": "OK", "content": "page B"}
        ]

        with patch("builtins.open", mock_open(read_data=file_data)):
            self.producer.process_file("urls.txt")

        items = self._drain_queue()
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].content, "page A")
        self.assertFalse(items[0].finished)
        self.assertEqual(items[1].content, "page B")
        self.assertTrue(items[1].finished)

    def test_process_file_missing_error(self):
        """Verifies FileNotFoundError produces a terminal ProducerItem with finished=True."""
        with patch("builtins.open", side_effect=FileNotFoundError("No such file or directory")):
            self.producer.process_file("missing_file.txt")

        items = self._drain_queue()
        self.assertEqual(len(items), 1)
        self.assertTrue(items[0].finished)
        self.assertIsNone(items[0].html_code)
        self.assertIn("File Error: No such file or directory", items[0].message)


class TestProducerNetworkHandling(unittest.TestCase):
    """Verifies the internal _fetch_url method against HTTP, URL, and generic errors."""

    def setUp(self):
        self.producer = Producer(out_queue=Queue(), num_threads=1)

    @patch("urllib.request.urlopen")
    def test_fetch_url_success(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.headers.get_content_charset.return_value = "utf-8"
        mock_response.read.return_value = b"<html><body>Hello</body></html>"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        res = self.producer._fetch_url("http://example.com")
        self.assertEqual(res["html_code"], 200)
        self.assertEqual(res["message"], "OK")
        self.assertEqual(res["content"], "<html><body>Hello</body></html>")

    @patch("urllib.request.urlopen")
    def test_fetch_url_custom_charset(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.headers.get_content_charset.return_value = "iso-8859-1"
        mock_response.read.return_value = "café".encode("iso-8859-1")
        mock_urlopen.return_value.__enter__.return_value = mock_response

        res = self.producer._fetch_url("http://example.com/latin")
        self.assertEqual(res["content"], "café")

    @patch("urllib.request.urlopen")
    def test_fetch_url_http_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="http://example.com/404",
            code=404,
            msg="Not Found",
            hdrs=Message(),
            fp=None
        )

        res = self.producer._fetch_url("http://example.com/404")
        self.assertEqual(res["html_code"], 404)
        self.assertIn("HTTP Error 404: Not Found", res["message"])
        self.assertIsNone(res["content"])

    @patch("urllib.request.urlopen")
    def test_fetch_url_dns_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.URLError("Name or service not known")

        res = self.producer._fetch_url("http://nonexistent.domain")
        self.assertIsNone(res["html_code"])
        self.assertIn("URL Error: Name or service not known", res["message"])
        self.assertIsNone(res["content"])

    @patch("urllib.request.urlopen")
    def test_fetch_url_timeout_exception(self, mock_urlopen):
        mock_urlopen.side_effect = TimeoutError("Timed out")

        res = self.producer._fetch_url("http://timeout.domain")
        self.assertIsNone(res["html_code"])
        self.assertIn("Error: Timed out", res["message"])
        self.assertIsNone(res["content"])


class TestProducerThreadIntegration(unittest.TestCase):
    """Simulates threading model where producer runs in a separate thread and consumer drains."""

    @patch.object(Producer, "_fetch_url")
    def test_threaded_execution_and_consumer_termination(self, mock_fetch):
        mock_fetch.side_effect = lambda url: {"html_code": 200, "message": "OK", "content": f"data for {url}"}

        q = Queue()
        producer = Producer(out_queue=q, num_threads=2)
        urls = "https://t1.com\nhttps://t2.com\nhttps://t3.com"

        prod_thread = threading.Thread(target=producer.process_string, args=(urls,))
        prod_thread.start()

        consumed = []
        while True:
            item = q.get()
            consumed.append(item)
            q.task_done()
            if item.finished:
                break

        prod_thread.join(timeout=2.0)
        self.assertFalse(prod_thread.is_alive(), "Producer thread failed to terminate")
        self.assertEqual(len(consumed), 3)
        self.assertTrue(consumed[-1].finished)
        self.assertEqual(consumed[0].content, "data for https://t1.com")
        self.assertEqual(consumed[2].content, "data for https://t3.com")


if __name__ == "__main__":
    unittest.main()