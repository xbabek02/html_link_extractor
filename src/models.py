from dataclasses import dataclass
from typing import List, Optional

@dataclass
class ProducerItem:
    """Structure yielded to the consumer queue."""
    url: Optional[str]
    html_code: Optional[int]
    message: str
    content: Optional[str]
    finished: bool