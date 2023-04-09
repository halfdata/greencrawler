"""Implements Crawler class."""

import re
from datetime import datetime
from typing import Any, Optional, final

import asyncio
import aiohttp

from sqlalchemy import create_engine, Engine, Row
from sqlalchemy import select, insert, update
from sqlalchemy import func

from .classes import CrawlingMode, TasksState, URLData
from .data import metadata_obj, url_table, token_table

HTTP_VSTATUS_NO_RESPONSE = 0
HTTP_VSTATUS_NOT_HTML = 13


class CrawlerException(Exception):
    """Base class for exceptions used by Crawler."""


class Crawler:
    """Base class for web crawler."""
    initial_url_data: URLData
    number_of_tasks: int = 3
    crawling_mode: CrawlingMode
    token_id: int = None
    engine: Engine = create_engine("sqlite:///db.sqlite3")
    _busy: bool = False

    _forbidden_domains: list[str] = []
    _forbidden_keywords: list[str] = []
    _allowed_extensions: list[str] = ["htm", "html", "shtml", "asp", "aspx", "jsp",
        "jspx", "php", "php5", "php4", "txt", ""]


    def __init__(self, *, number_of_tasks: int = 3) -> None:
        self.number_of_tasks = number_of_tasks
        self.tasks_state = TasksState(number_of_tasks)
        metadata_obj.create_all(self.engine)

    def get_forbidden_domains(self) -> list[str]:
        """Retruns list of forbidden domains."""
        return self._forbidden_domains

    def set_forbidden_domains(self, domains: list[str]) -> None:
        """Set list of forbidden domains. Use regular expressions to define domain."""
        self._forbidden_domains = []
        for domain in domains:
            try:
                re.compile(domain)
            except re.error as error:
                raise CrawlerException(f"Invalid regualr expression: {domain}") from error
            self._forbidden_domains.append(domain.lower())

    def get_forbidden_keywords(self) -> list[str]:
        """Retruns list of forbidden keywords."""
        return self._forbidden_keywords

    def set_forbidden_keywords(self, keywords: list[str]) -> None:
        """Set list of forbidden keywords. Use regular expressions to define keyword."""
        self._forbidden_keywords = []
        for keyword in keywords:
            try:
                re.compile(keyword)
            except re.error as error:
                raise CrawlerException(f"Invalid regualr expression: {keyword}") from error
            self._forbidden_keywords.append(keyword.lower())

    @final
    def _get_next_url(self) -> Optional[Row[tuple]]:
        """Returns next URL from url_table for given token."""
        with self.engine.connect() as connection:
            statement = (select(url_table)
                    .where(url_table.c.token_id == self.token_id)
                    .where(url_table.c.processed.is_(False))
                    .where(url_table.c.fetched.is_(False))
                    .order_by(url_table.c.id)
                    .limit(1))
            url = connection.execute(statement).first()
        return url

    @final
    def _check_hash_exists(self, hash_id: str) -> bool:
        """Checks if hash_id already exists."""
        with self.engine.connect() as connection:
            statement = (select(url_table)
                    .where(url_table.c.token_id == self.token_id)
                    .where(url_table.c.hash_id == hash_id)
                    .limit(1))
            url = connection.execute(statement).first()
        return bool(url)

    @final
    def _add_url(self, url_data: URLData) -> None:
        """Insert new URL record into url_table."""
        with self.engine.connect() as connection:
            connection.execute(
                insert(url_table).values(
                    token_id=self.token_id,
                    url=url_data.full_url,
                    hash_id=url_data.hash
                ))
            connection.commit()

    @final
    def _set_url_as_fetched(self, url: Row[tuple]) -> None:
        """Mark URL as fetched in database."""
        with self.engine.connect() as connection:
            connection.execute(
                update(url_table)
                .where(url_table.c.id == url.id)
                .values(fetched=True))
            connection.commit()

    @final
    def _set_url_as_processed(self, url: Row[tuple], status: int) -> None:
        """Mark URL as processed in database."""
        with self.engine.connect() as connection:
            connection.execute(
                update(url_table)
                .where(url_table.c.id == url.id)
                .values(processed=True, status=status))
            connection.commit()

    @final
    def _process_url(self, parent_url: str, html: str) -> None:
        """Extract URLs from web page and put them into queue."""
        regex = re.compile(r'href=[\"\\\']+([^\"\\\']+)', re.IGNORECASE)
        urls = re.findall(regex, html)
        for candidate_url in urls:
            if candidate_url.startswith("#"):
                continue
            candidate_data = URLData(candidate_url, parent_url)
            if not bool(candidate_data):
                continue
            if self._check_hash_exists(candidate_data.hash):
                continue
            if (self.crawling_mode == CrawlingMode.DOMAIN_ONLY and
                candidate_data.domain != self.initial_url_data.domain):
                continue
            if (self.crawling_mode == CrawlingMode.DOMAIN_AND_SUBDOMAINS and
                not candidate_data.domain.endswith(self.initial_url_data.domain)):
                continue
            domain_forbidden = False
            for pattern in self._forbidden_domains:
                regex = re.compile(rf"^([a-z0-9-]+\.)*({pattern})$", re.IGNORECASE)
                if re.match(regex, candidate_data.domain):
                    domain_forbidden = True
                    break
            if domain_forbidden:
                continue
            keyword_forbidden = False
            for pattern in self._forbidden_keywords:
                regex = re.compile(rf".*({pattern}).*", re.IGNORECASE)
                if re.match(regex, candidate_data.full_url):
                    keyword_forbidden = True
                    break
            if keyword_forbidden:
                continue
            extension = ""
            if "." in candidate_data.details.path:
                extension = candidate_data.details.path.split(".")[-1].lower()
                if len(extension) > 5:
                    extension = ""
            if extension not in self._allowed_extensions:
                continue

            self._add_url(candidate_data)
        self.custom_process_url(parent_url, html)

    def custom_process_url(self, url: str, html: str) -> None:
        """Process content of the page."""

    def active_tokens(self) -> list[dict[str,Any]]:
        """Returns list of open tokens."""
        tokens = []
        with self.engine.connect() as connection:
            total_urls_statement = (select(func.count().label("total_urls"), url_table.c.token_id)
                .select_from(url_table)
                 .group_by(url_table.c.token_id).subquery())
            not_processed_statement = (
                select(func.count().label("not_processed_urls"), url_table.c.token_id)
                .select_from(url_table)
                .where(url_table.c.processed.is_(False))
                .group_by(url_table.c.token_id).subquery())
            statement = (select(
                        token_table,
                        total_urls_statement.c.total_urls,
                        not_processed_statement.c.not_processed_urls)
                    .join(
                        not_processed_statement,
                        not_processed_statement.c.token_id == token_table.c.id)
                    .join(
                        total_urls_statement,
                        total_urls_statement.c.token_id == token_table.c.id,
                        isouter=True))
            rows = connection.execute(statement)
            tokens = [{
                "id": r.id,
                "url": r.url,
                "created": r.created,
                "total_urls": r.total_urls,
                "not_processed_urls": r.not_processed_urls}  for r in rows]
        return tokens

    async def task(self, task_idx):
        """Process token URL."""
        while True:
            if bool(self.tasks_state):
                break
            url = self._get_next_url()
            if not url:
                self.tasks_state.set_free_task(task_idx)
                await asyncio.sleep(1)
                continue
            self.tasks_state.reset()
            self._set_url_as_fetched(url)
            status = 0
            html = ""
            async with aiohttp.ClientSession() as aiohttp_session:
                try:
                    async with aiohttp_session.get(url.url) as response:
                        if "text/html" in response.headers.get("Content-Type", ""):
                            status = response.status
                            html = await response.text()
                        else:
                            status = HTTP_VSTATUS_NOT_HTML
                except aiohttp.ClientError:
                    pass
                except asyncio.TimeoutError:
                    status = HTTP_VSTATUS_NO_RESPONSE
            if html and (status >= 200 or status <= 299):
                self._process_url(url.url, html)
            self._set_url_as_processed(url, status)
            print(f"{url.url} [status: {status}]")

    async def start(self, *, initial_url: str,
                    crawling_mode: CrawlingMode = CrawlingMode.DOMAIN_ONLY) -> None:
        """Start crawling."""
        if self._busy:
            print("Crawling is in process.")
            return

        self.initial_url_data = URLData(initial_url)
        if not bool(self.initial_url_data):
            raise CrawlerException("Valid initial URL required.")
        self.crawling_mode = crawling_mode

        with self.engine.connect() as connection:
            token_id = connection.execute(
                insert(token_table).values(
                    url=self.initial_url_data.full_url,
                    mode=self.crawling_mode,
                    created=datetime.utcnow()
                )).inserted_primary_key.id
            connection.execute(
                insert(url_table).values(
                    token_id=token_id,
                    url=self.initial_url_data.full_url,
                    hash_id=self.initial_url_data.hash
                ))
            connection.commit()

        await self.resume(token_id=token_id)

    async def resume(self, *, token_id: int) -> None:
        """Resume crawling."""
        if self._busy:
            print("Crawling is in process.")
            return
        if not token_id:
            raise CrawlerException("Requested token not found.")
        
        with self.engine.connect() as connection:
            statement = select(token_table).where(token_table.c.id == token_id)
            token = connection.execute(statement).first()
            if not token:
                raise CrawlerException("Requested token not found.")
            connection.execute(update(url_table)
                .where(url_table.c.token_id == token_id)
                .where(url_table.c.processed.is_(False))
                .where(url_table.c.fetched.is_(True))
                .values(fetched=False))
            connection.commit()
            statement = (select(token_table)
                .join_from(token_table, url_table)
                .where(token_table.c.id == token_id)
                .where(url_table.c.processed.is_(False)))
            token = connection.execute(statement).first()
        if not token:
            print("Crawling finished!")
            return

        self.crawling_mode = token.mode
        self.initial_url_data = URLData(token.url)
        self.token_id = token_id
        self._busy = True
        tasks = [self.task(idx) for idx in range(self.tasks_state.size)]
        await asyncio.gather(*tasks)
        self._busy = False
        print("Crawling finished!")
