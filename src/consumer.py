import csv
from html.parser import HTMLParser
import json
from queue import Queue
import re
import sys
from typing import Any, List, Optional
from urllib.parse import urljoin

from producer import ProducerItem

VOID_ELEMENTS = {
    "area", "base", "br", "col", "embed", "hr", "img",
    "input", "link", "meta", "param", "source", "track", "wbr",
}


class Element:
    """Represents an open HTML element in the tag stack."""
    __slots__ = ("tag", "attrs", "classes", "id")

    def __init__(self, tag: str, attrs: list):
        self.tag = tag.lower()
        self.attrs = {k.lower(): (v or "") for k, v in attrs}
        self.id = self.attrs.get("id", "")
        class_attr = self.attrs.get("class", "")
        self.classes = set(class_attr.split()) if class_attr else set()


class CompoundSelector:
    """Matches a single selector unit like 'div.products', '#main', or 'nav'."""

    def __init__(self, raw: str):
        self.tag: Optional[str] = None
        self.id: Optional[str] = None
        self.classes: List[str] = []
        self.attrs: List[tuple] = []

        raw = raw.strip()

        # Attribute selector extraction: [attr] or [attr=val]
        attr_pattern = re.compile(r'\[([a-zA-Z0-9_\-]+)(?:=([\'"]?)(.*?)\2)?\]')
        for match in attr_pattern.finditer(raw):
            attr_name = match.group(1).lower()
            attr_val = match.group(3) if match.group(3) is not None else None
            self.attrs.append((attr_name, attr_val))

        stripped = attr_pattern.sub('', raw)

        # Tag name extraction
        tag_match = re.match(r'^([a-zA-Z0-9_\-]+|\*)', stripped)
        if tag_match:
            t = tag_match.group(1)
            if t != '*':
                self.tag = t.lower()
            stripped = stripped[tag_match.end():]

        # ID (#) and Class (.) extraction
        for prefix, name in re.findall(r'([.#])([a-zA-Z0-9_\-]+)', stripped):
            if prefix == '#':
                self.id = name
            elif prefix == '.':
                self.classes.append(name)

    def matches(self, elem: Element) -> bool:
        if self.tag and elem.tag != self.tag:
            return False
        if self.id and elem.id != self.id:
            return False
        for cls in self.classes:
            if cls not in elem.classes:
                return False
        for attr_name, attr_val in self.attrs:
            if attr_name not in elem.attrs:
                return False
            if attr_val is not None and elem.attrs[attr_name] != attr_val:
                return False
        return True


class DescendantSelector:
    """Matches a whitespace-separated descendant chain like 'div.products .row'."""

    def __init__(self, raw: str):
        tokens = raw.strip().split()
        self.compounds = [CompoundSelector(tok) for tok in tokens if tok]

    def matches_chain(self, elements: List[Element]) -> bool:
        if not self.compounds or not elements:
            return False

        # Rightmost selector must match the terminal element
        if not self.compounds[-1].matches(elements[-1]):
            return False

        compound_idx = len(self.compounds) - 2
        elem_idx = len(elements) - 2

        # Walk up the ancestor chain to match remaining compounds
        while compound_idx >= 0 and elem_idx >= 0:
            if self.compounds[compound_idx].matches(elements[elem_idx]):
                compound_idx -= 1
            elem_idx -= 1

        return compound_idx < 0


class SelectorFilter:
    """Evaluates elements against comma-separated CSS selectors."""

    def __init__(self, selector_str: Optional[str]):
        self.selectors: List[DescendantSelector] = []
        if selector_str and selector_str.strip():
            for sel in selector_str.split(","):
                sel = sel.strip()
                if sel:
                    self.selectors.append(DescendantSelector(sel))

    def is_active(self) -> bool:
        return len(self.selectors) > 0

    def matches(self, stack: List[Element], current_elem: Element) -> bool:
        if not self.is_active():
            return True

        for selector in self.selectors:
            # 1. Matches if the link itself matches the selector
            if selector.matches_chain(stack + [current_elem]):
                return True

            # 2. Matches if the link is nested inside any matching ancestor
            for i in range(len(stack)):
                if selector.matches_chain(stack[: i + 1]):
                    return True

        return False


class LinkExtractor(HTMLParser):
    def __init__(self, base_url: Optional[str] = None, selector: Optional[str] = None):
        super().__init__()
        self.base_url = base_url
        self.selector_filter = SelectorFilter(selector)
        self.links: List[str] = []
        self.stack: List[Element] = []

    def handle_starttag(self, tag: str, attrs: list):
        elem = Element(tag, attrs)

        if elem.tag == "a":
            href = elem.attrs.get("href")
            if href:
                clean_href = href.strip()
                if self._is_valid_hyperlink(clean_href):
                    if self.selector_filter.matches(self.stack, elem):
                        resolved_url = urljoin(self.base_url, clean_href) if self.base_url else clean_href
                        self.links.append(resolved_url)

        if elem.tag not in VOID_ELEMENTS:
            self.stack.append(elem)

    def handle_endtag(self, tag: str):
        tag_lower = tag.lower()
        if tag_lower in VOID_ELEMENTS:
            return

        # Pop up to and including the closing tag
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i].tag == tag_lower:
                self.stack = self.stack[:i]
                break

    @staticmethod
    def _is_valid_hyperlink(href: str) -> bool:
        lowered = href.lower()
        return not (
            lowered.startswith("#")
            or lowered.startswith("javascript:")
            or lowered.startswith("mailto:")
            or lowered.startswith("tel:")
        )


class Consumer:
    def __init__(
        self,
        in_queue: Queue,
        out_stream=sys.stdout,
        selector: Optional[str] = None,
    ):
        self.in_queue = in_queue
        self.out_stream = out_stream
        self.selector = selector

    def run(self) -> None:
        writer = csv.writer(self.out_stream)
        writer.writerow(["input_link", "links", "status", "message"])
        self.out_stream.flush()

        while True:
            item: ProducerItem = self.in_queue.get()
            try:
                self._process_item(item, writer)
            finally:
                self.in_queue.task_done()

            if item.finished:
                break

    def _process_item(self, item: ProducerItem, writer: Any) -> None:
        input_link = item.url or ""
        parsed_links: List[str] = []

        if (
            item.html_code is not None
            and 200 <= item.html_code < 300
            and item.content is not None
        ):
            try:
                parser = LinkExtractor(base_url=item.url, selector=self.selector)
                parser.feed(item.content)
                parsed_links = list(dict.fromkeys(parser.links))
                status = 1
                message = "OK"
            except Exception as e:
                status = 0
                message = f"Parse Error: {str(e)}"
        else:
            status = 0
            message = item.message

        writer.writerow([input_link, json.dumps(parsed_links), status, message])
        self.out_stream.flush()