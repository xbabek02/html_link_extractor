from pathlib import Path
import sys
import unittest

src_path = str(Path(__file__).resolve().parent.parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from consumer import LinkExtractor

HTML_FIXTURE = """
<html>
  <body>
    <nav class="navbar">
      <a class="nav-brand" href="/home">Home</a>
      <ul class="dropdown-menu">
        <li><a href="/api/docs">API Docs</a></li>
      </ul>
    </nav>
    <div class="container">
      <div class="filters">
        <a href="/category/books">Books</a>
      </div>
      <div class="products">
        <div class="item">
          <a href="/product/1">Product 1</a>
        </div>
        <div class="item">
          <a href="/product/2">Product 2</a>
        </div>
      </div>
    </div>
  </body>
</html>
"""

class TestSelectorFiltering(unittest.TestCase):
    def test_no_selector_extracts_all(self):
        extractor = LinkExtractor(base_url="https://site.com", selector=None)
        extractor.feed(HTML_FIXTURE)
        self.assertEqual(len(extractor.links), 5)

    def test_class_container_filter(self):
        extractor = LinkExtractor(base_url="https://site.com", selector=".products")
        extractor.feed(HTML_FIXTURE)
        self.assertEqual(extractor.links, [
            "https://site.com/product/1",
            "https://site.com/product/2",
        ])

    def test_descendant_selector(self):
        extractor = LinkExtractor(base_url="https://site.com", selector="nav .dropdown-menu")
        extractor.feed(HTML_FIXTURE)
        self.assertEqual(extractor.links, ["https://site.com/api/docs"])

    def test_tag_and_class_anchor_selector(self):
        extractor = LinkExtractor(base_url="https://site.com", selector="a.nav-brand")
        extractor.feed(HTML_FIXTURE)
        self.assertEqual(extractor.links, ["https://site.com/home"])

    def test_unmatched_selector_returns_empty(self):
        extractor = LinkExtractor(base_url="https://site.com", selector="#nonexistent")
        extractor.feed(HTML_FIXTURE)
        self.assertEqual(extractor.links, [])


if __name__ == "__main__":
    unittest.main()