"""Different helpers."""

import hashlib
import re
from typing import Optional
from enum import Enum
from urllib.parse import ParseResult
from urllib.parse import urlparse, parse_qs


class CrawlingMode(Enum):
    """Enum defining crawling modes."""
    DOMAIN_ONLY = 1
    DOMAIN_AND_SUBDOMAINS = 2
    ALL = 3


class TasksState:
    """Keep the state of crawler tasks."""
    tasks: list[bool]
    size: int

    def __init__(self, number_of_tasks: int) -> None:
        self.size = number_of_tasks
        self.reset()

    def set_free_task(self, task_id: int) -> None:
        """Mark task as free."""
        self.tasks[task_id] = True

    def reset(self) -> None:
        """Mark all tasks as busy."""
        self.tasks = [False] * self.size

    def __bool__(self):
        """Returns True if all tasks are free."""
        return all(self.tasks)


class URLData:
    """URL parser."""
    original_url: str
    full_url: str = None
    details: ParseResult

    def __init__(self, url: str, parent_url: str = None) -> None:
        regex = re.compile(
                r'^(?:http)s?://' # http:// or https://
                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|'
                r'[A-Z0-9-]{2,}\.?)|' #domain...
                r'localhost|' #localhost...
                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
                r'(?::\d+)?' # optional port
                r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        self.original_url = url
        self.full_url = url.split('#')[0]
        self.details = urlparse(self.full_url)
        if re.match(regex, self.full_url) is not None:
            return
        if self.details.scheme or re.match(regex, parent_url) is None:
            self.full_url = None
            return

        parent_details = urlparse(parent_url)
        if not url or url.startswith('#'):
            self.full_url = None
            return
        if url.startswith('//'):
            self.full_url = f"{parent_details.scheme}:{url}"
        elif url.startswith('/'):
            self.full_url = f"{parent_details.scheme}://{parent_details.netloc}{url}"
        elif url.startswith('?'):
            self.full_url = (f"{parent_details.scheme}://{parent_details.netloc}"
                             f"{parent_details.path if parent_details.path else '/'}"
                             f"{url}")
        else:
            path_parts = parent_details.path.split('/')
            path_parts[-1] = url
            self.full_url = (f"{parent_details.scheme}://{parent_details.netloc}"
                             f"{'/' if len(path_parts) == 1 else ''}"
                             f"{'/'.join(path_parts)}")

        self.full_url = self.full_url.split('#')[0]
        if re.match(regex, self.full_url) is None:
            self.full_url = None
            return
        self.details = urlparse(self.full_url)

    def __bool__(self) -> bool:
        """Returns True is URL is valid http/https URL."""
        return self.full_url is not None

    @property
    def scheme(self) -> str:
        """Returns scheme of the URL."""
        return self.details.scheme

    @property
    def is_http(self) -> bool:
        """Returns True if URL's scheme is http or https."""
        return self.details.scheme.startswith('http')

    @property
    def domain(self) -> str:
        """Returns domain of the URL."""
        return self.details.hostname

    @property
    def hash(self) -> Optional[str]:
        """Returns hash of the URL."""
        if not self.is_http:
            return None
        netloc = self.details.netloc.lower()
        parsed_query = parse_qs(self.details.query)
        query_parts = []
        for key, value in sorted(parsed_query.items()):
            query_parts.append(f"{key.lower()}={'#'.join(sorted(map(lambda s: s.lower(), value)))}")
        url_parts_for_hash = [
            self.scheme,
            netloc if not netloc.startswith("www.") else netloc[4:],
            re.sub("//+", "/", self.details.path.lower()) if self.details.path else '/',
            '&'.join(query_parts)
        ]
        return hashlib.md5('>'.join(url_parts_for_hash).encode()).hexdigest()
