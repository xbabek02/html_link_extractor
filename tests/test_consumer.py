import csv
import io
import json
from queue import Queue
import unittest
from unittest.mock import patch

from pathlib import Path
import sys

# Ensure src/ is on sys.path so modules inside src resolve their peer imports
src_path = str(Path(__file__).resolve().parent.parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from consumer import Consumer, LinkExtractor
from producer import ProducerItem


class TestLinkExtractor(unittest.TestCase):
    """Tests HTML parsing, attribute filtering, and URL resolution."""

    def test_extract_absolute_links(self):
        html = '<a href="https://example.com/page1">One</a><a href="https://example.com/page2">Two</a>'
        extractor = LinkExtractor()
        extractor.feed(html)
        self.assertEqual(extractor.links, ["https://example.com/page1", "https://example.com/page2"])

    def test_resolve_relative_links(self):
        html = '<a href="/about">About</a><a href="contact.html">Contact</a>'
        extractor = LinkExtractor(base_url="https://example.com/sub/")
        extractor.feed(html)
        self.assertEqual(extractor.links, ["https://example.com/about", "https://example.com/sub/contact.html"])

    def test_filter_non_navigational_schemes(self):
        html = """
        <a href="#section">Fragment</a>
        <a href="javascript:void(0)">JS Action</a>
        <a href="mailto:support@example.com">Email</a>
        <a href="tel:+123456789">Call</a>
        <a href="https://valid.com">Valid</a>
        """
        extractor = LinkExtractor()
        extractor.feed(html)
        self.assertEqual(extractor.links, ["https://valid.com"])

    def test_ignore_other_tags_with_href_or_src(self):
        html = """
        <link rel="stylesheet" href="style.css">
        <script src="app.js"></script>
        <img src="pic.jpg" alt="photo">
        <a href="https://valid.org">Valid Link</a>
        """
        extractor = LinkExtractor(base_url="https://valid.org")
        extractor.feed(html)
        self.assertEqual(extractor.links, ["https://valid.org"])

    def test_anchor_without_href(self):
        html = '<a>Nameless anchor</a><a href="https://target.com">Target</a>'
        extractor = LinkExtractor()
        extractor.feed(html)
        self.assertEqual(extractor.links, ["https://target.com"])


class TestConsumer(unittest.TestCase):
    """Tests queue consumption, item translation, and CSV stream generation."""

    def setUp(self):
        self.queue = Queue()
        self.output_buffer = io.StringIO()
        self.consumer = Consumer(in_queue=self.queue, out_stream=self.output_buffer)

    def _get_csv_rows(self):
        self.output_buffer.seek(0)
        return list(csv.reader(self.output_buffer))

    def test_csv_header_written_on_init(self):
        # Push single finished sentinel to exit immediately
        self.queue.put(ProducerItem(url=None, html_code=None, message="Done", content=None, finished=True))
        self.consumer.run()

        rows = self._get_csv_rows()
        self.assertEqual(rows[0], ["input_link", "links", "status", "message"])

    def test_process_success_item_with_deduplication(self):
        html = """
        <a href="/docs">Docs</a>
        <a href="/docs">Docs Duplicate</a>
        <a href="https://other.com">Other</a>
        """
        item = ProducerItem(
            url="https://base.org",
            html_code=200,
            message="OK",
            content=html,
            finished=True,
        )
        self.queue.put(item)
        self.consumer.run()

        rows = self._get_csv_rows()
        self.assertEqual(len(rows), 2)  # Header + 1 row

        input_link, links_json, status, message = rows[1]
        self.assertEqual(input_link, "https://base.org")
        self.assertEqual(status, "1")
        self.assertEqual(message, "OK")

        links = json.loads(links_json)
        self.assertEqual(links, ["https://base.org/docs", "https://other.com"])

    def test_process_http_failure_item(self):
        item = ProducerItem(
            url="https://broken.com",
            html_code=404,
            message="HTTP Error 404: Not Found",
            content=None,
            finished=True,
        )
        self.queue.put(item)
        self.consumer.run()

        rows = self._get_csv_rows()
        self.assertEqual(len(rows), 2)

        input_link, links_json, status, message = rows[1]
        self.assertEqual(input_link, "https://broken.com")
        self.assertEqual(links_json, "[]")
        self.assertEqual(status, "0")
        self.assertEqual(message, "HTTP Error 404: Not Found")

    def test_process_network_error_item(self):
        item = ProducerItem(
            url="https://unreachable.org",
            html_code=None,
            message="URL Error: Name or service not known",
            content=None,
            finished=True,
        )
        self.queue.put(item)
        self.consumer.run()

        rows = self._get_csv_rows()
        input_link, links_json, status, message = rows[1]
        self.assertEqual(status, "0")
        self.assertEqual(message, "URL Error: Name or service not known")
        self.assertEqual(json.loads(links_json), [])

    def test_parser_exception_graceful_handling(self):
        item = ProducerItem(
            url="https://malformed.com",
            html_code=200,
            message="OK",
            content="<html><body>crash</body></html>",
            finished=True,
        )
        self.queue.put(item)

        with patch.object(LinkExtractor, "feed", side_effect=RuntimeError("Parsing failed")):
            self.consumer.run()

        rows = self._get_csv_rows()
        input_link, links_json, status, message = rows[1]
        self.assertEqual(status, "0")
        self.assertIn("Parse Error: Parsing failed", message)
        self.assertEqual(json.loads(links_json), [])

    def test_multi_item_stream_lifecycle(self):
        items = [
            ProducerItem(
                url="https://site1.com",
                html_code=200,
                message="OK",
                content='<a href="/p1">P1</a>',
                finished=False,
            ),
            ProducerItem(
                url="https://site2.com",
                html_code=500,
                message="HTTP Error 500: Server Error",
                content=None,
                finished=False,
            ),
            ProducerItem(
                url="https://site3.com",
                html_code=200,
                message="OK",
                content='<a href="/p3">P3</a>',
                finished=True,
            ),
        ]

        for item in items:
            self.queue.put(item)

        self.consumer.run()

        rows = self._get_csv_rows()
        self.assertEqual(len(rows), 4)  # Header + 3 data rows

        # Row 1 (Site 1)
        self.assertEqual(rows[1][0], "https://site1.com")
        self.assertEqual(rows[1][2], "1")
        self.assertEqual(json.loads(rows[1][1]), ["https://site1.com/p1"])

        # Row 2 (Site 2)
        self.assertEqual(rows[2][0], "https://site2.com")
        self.assertEqual(rows[2][2], "0")
        self.assertEqual(rows[2][3], "HTTP Error 500: Server Error")

        # Row 3 (Site 3)
        self.assertEqual(rows[3][0], "https://site3.com")
        self.assertEqual(rows[3][2], "1")
        self.assertEqual(json.loads(rows[3][1]), ["https://site3.com/p3"])


if __name__ == "__main__":
    unittest.main()