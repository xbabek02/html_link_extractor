import csv
import io
import json
import os
from pathlib import Path
from queue import Queue
import sys
import threading
import unittest
from unittest.mock import patch

# Ensure src path is accessible when running this file directly
src_path = str(Path(__file__).resolve().parent.parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from consumer import Consumer
from producer import Producer

WEB_SCRAPING_DEV_PRODUCTS_HTML = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://web-scraping.dev/assets/css/main.css"/>
    <link rel="stylesheet" href="https://web-scraping.dev/assets/css/bootstrap.min.css"/>
    <link rel="stylesheet" href="https://web-scraping.dev/assets/css/bootstrap-icons.css"/>
    <link rel="stylesheet" href="https://web-scraping.dev/assets/css/highlight-nord.css"/>
    <link rel="icon" href="https://web-scraping.dev/assets/media/icon.png" type="image/png" />
    <script src="https://web-scraping.dev/assets/js/cash.min.js"></script>
    <script src="https://web-scraping.dev/assets/js/main.js"></script>
    <script src="https://web-scraping.dev/assets/js/bootstrap.js"></script>
    <title>web-scraping.dev product page 1</title>
    <meta name="description" content="Mock product pagination page 1 of None category for web scraper testing" />
    <link rel="canonical" href="https://web-scraping.dev/products" />
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-light">
      <div class="container-fluid">
        <a class="navbar-brand" href="https://web-scraping.dev/">web-scraping.dev</a>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarContent">
          <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarContent">
          <ul class="navbar-nav me-auto mb-2 mb-lg-0">
            <li class="nav-item dropdown">
              <a class="nav-link dropdown-toggle text-center" href="#" id="headersDropdown" data-bs-toggle="dropdown">
                Docs
              </a>
              <ul class="dropdown-menu" aria-labelledby="headersDropdown">
                <li><a class="dropdown-item" href="/docs">API</a></li>
                <li><a class="dropdown-item" href="/api/graphql">Graphql</a></li>
              </ul>
            </li>
            <li class="nav-item"><a class="nav-link" href="https://web-scraping.dev/products">Products</a></li>
            <li class="nav-item"><a class="nav-link" href="https://web-scraping.dev/reviews">Reviews</a></li>
            <li class="nav-item"><a class="nav-link" href="https://web-scraping.dev/testimonials">Testimonials</a></li>
            <li class="nav-item"><a class="nav-link" href="https://web-scraping.dev/file-download">File Download</a></li>
            <li class="nav-item"><a class="nav-link" href="https://web-scraping.dev/mcp-tools">MCP Tools</a></li>
          </ul>
          <ul class="navbar-nav mb-2 mb-lg-0">
            <li class="nav-item"><a class="nav-link" href="https://web-scraping.dev/login">login</a></li>
            <li class="nav-item cart-preview">
              <div class="cart-icon">
                <a href="/cart"><button class="btn btn-primary btn-rounded cart-continue btn-success">Continue</button></a>
              </div>
            </li>
          </ul>
        </div>
      </div>
    </nav>
    <div class="main">
      <div class="container">
        <div class="filters">
          <a>all</a>
          <a href="https://web-scraping.dev/products?category=apparel">apparel</a>
          <a href="https://web-scraping.dev/products?category=consumables">consumables</a>
          <a href="https://web-scraping.dev/products?category=household">household</a>
        </div>
        <div class="products-wrap">
          <div class="products">
            <div class="row product">
              <div class="col-8 description">
                <h3><a href="https://web-scraping.dev/product/1">Box of Chocolate Candy</a></h3>
              </div>
            </div>
            <div class="row product">
              <div class="col-8 description">
                <h3><a href="https://web-scraping.dev/product/2">Dark Red Energy Potion</a></h3>
              </div>
            </div>
            <div class="row product">
              <div class="col-8 description">
                <h3><a href="https://web-scraping.dev/product/3">Teal Energy Potion</a></h3>
              </div>
            </div>
            <div class="row product">
              <div class="col-8 description">
                <h3><a href="https://web-scraping.dev/product/4">Red Energy Potion</a></h3>
              </div>
            </div>
            <div class="row product">
              <div class="col-8 description">
                <h3><a href="https://web-scraping.dev/product/5">Blue Energy Potion</a></h3>
              </div>
            </div>
          </div>
          <div class="paging">
            <a href="https://web-scraping.dev/products?page=1">&lt;</a>
            <a>1</a>
            <a href="https://web-scraping.dev/products?page=2">2</a>
            <a href="https://web-scraping.dev/products?page=3">3</a>
            <a href="https://web-scraping.dev/products?page=4">4</a>
            <a href="https://web-scraping.dev/products?page=5">5</a>
            <a href="https://web-scraping.dev/products?page=2">&gt;</a>
          </div>
        </div>
      </div>
    </div>
    <div class="container">
      <footer class="d-flex flex-wrap justify-content-between align-items-center py-3 my-4 border-top">
        <div class="col-md-4 d-flex align-items-center">
          <a href="https://scrapfly.io/academy">SCRAPFLY ACADEMY</a>
        </div>
        <ul class="nav col-md-8 justify-content-end list-unstyled d-flex align-items-center gap-3">
          <li><a href="/docs">API Docs</a></li>
          <li><a href="/sitemap.xml">Sitemap</a></li>
          <li><a href="https://scrapfly.io/blog">Blog</a></li>
          <li><a href="https://github.com/scrapfly">GitHub</a></li>
          <li><a href="https://web-scraping.dev/">ScrapFly</a></li>
        </ul>
      </footer>
    </div>
  </body>
</html>"""


class TestFullPipeline(unittest.TestCase):
    """End-to-end integration tests connecting Producer and Consumer concurrently."""

    def _run_pipeline(self, raw_input_urls: str, fetch_side_effect=None) -> list:
        """Executes the pipeline. If fetch_side_effect is None, performs live HTTP requests."""
        shared_queue = Queue()
        output_buffer = io.StringIO()

        producer = Producer(out_queue=shared_queue, num_threads=2)
        consumer = Consumer(in_queue=shared_queue, out_stream=output_buffer)

        def execute():
            producer_thread = threading.Thread(
                target=producer.process_string,
                args=(raw_input_urls,),
                name="TestProducerThread",
            )
            consumer_thread = threading.Thread(
                target=consumer.run,
                name="TestConsumerThread",
            )

            producer_thread.start()
            consumer_thread.start()

            producer_thread.join(timeout=15.0)
            consumer_thread.join(timeout=15.0)

            self.assertFalse(producer_thread.is_alive(), "Producer thread failed to terminate")
            self.assertFalse(consumer_thread.is_alive(), "Consumer thread failed to terminate")

        if fetch_side_effect is not None:
            with patch.object(Producer, "_fetch_url", side_effect=fetch_side_effect):
                execute()
        else:
            execute()

        output_buffer.seek(0)
        return list(csv.reader(output_buffer))

    def test_live_web_scraping_dev_products(self):
        """Performs a real HTTP network request to https://web-scraping.dev/products."""
        target_url = "https://web-scraping.dev/products"

        # fetch_side_effect=None routes directly to live urllib.request
        rows = self._run_pipeline(target_url, fetch_side_effect=None)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], ["input_link", "links", "status", "message"])

        input_link, links_json, status, message = rows[1]
        extracted_links = json.loads(links_json)

        # Basic status and return checks
        self.assertEqual(input_link, target_url)
        self.assertEqual(status, "1", f"Live fetch failed with message: {message}")
        self.assertEqual(message, "OK")

        # Verify links returned by the live site
        self.assertTrue(len(extracted_links) > 0, "No links extracted from live page")
        self.assertIn("https://web-scraping.dev/products", extracted_links)
        self.assertTrue(any("/product/" in link for link in extracted_links), "Missing product detail links")

        # Exclusions remain enforced on live data
        self.assertNotIn("#", extracted_links)
        self.assertNotIn("", extracted_links)
        self.assertFalse(any(link.endswith(".css") for link in extracted_links))
        self.assertFalse(any(link.endswith(".js") for link in extracted_links))

    def test_mocked_web_scraping_dev_products_pipeline(self):
        """Verifies deterministic parsing against stored mock HTML."""
        target_url = "https://web-scraping.dev/products"

        def mock_fetch(url: str):
            if url == target_url:
                return {
                    "html_code": 200,
                    "message": "OK",
                    "content": WEB_SCRAPING_DEV_PRODUCTS_HTML,
                }
            return {"html_code": 404, "message": "Not Found", "content": None}

        rows = self._run_pipeline(target_url, fetch_side_effect=mock_fetch)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], ["input_link", "links", "status", "message"])

        input_link, links_json, status, message = rows[1]
        extracted_links = json.loads(links_json)

        self.assertEqual(input_link, target_url)
        self.assertEqual(status, "1")
        self.assertEqual(message, "OK")

        # Product detail link checks
        for product_id in range(1, 6):
            self.assertIn(f"https://web-scraping.dev/product/{product_id}", extracted_links)

        # Asset and non-anchor checks
        self.assertNotIn("#", extracted_links)
        self.assertNotIn("https://web-scraping.dev/assets/css/main.css", extracted_links)
        self.assertNotIn("https://web-scraping.dev/assets/js/main.js", extracted_links)

        # Deduplication check
        page_2_count = extracted_links.count("https://web-scraping.dev/products?page=2")
        self.assertEqual(page_2_count, 1)

    def test_multi_url_pipeline_success_and_failure(self):
        """Verifies multiple URLs handled in correct order with mixed outcomes."""
        url_success = "https://web-scraping.dev/products"
        url_fail = "https://web-scraping.dev/broken-page"
        input_data = f"{url_success}\n{url_fail}"

        def mock_fetch(url: str):
            if url == url_success:
                return {
                    "html_code": 200,
                    "message": "OK",
                    "content": '<html><body><a href="/page">Page</a></body></html>',
                }
            return {
                "html_code": 404,
                "message": "HTTP Error 404: Not Found",
                "content": None,
            }

        rows = self._run_pipeline(input_data, fetch_side_effect=mock_fetch)

        self.assertEqual(len(rows), 3)

        # Row 1: Success
        self.assertEqual(rows[1][0], url_success)
        self.assertEqual(json.loads(rows[1][1]), ["https://web-scraping.dev/page"])
        self.assertEqual(rows[1][2], "1")
        self.assertEqual(rows[1][3], "OK")

        # Row 2: Failure
        self.assertEqual(rows[2][0], url_fail)
        self.assertEqual(json.loads(rows[2][1]), [])
        self.assertEqual(rows[2][2], "0")
        self.assertEqual(rows[2][3], "HTTP Error 404: Not Found")


if __name__ == "__main__":
    unittest.main()