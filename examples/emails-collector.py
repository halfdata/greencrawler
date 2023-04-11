"""Emails collector."""
import sys
sys.path.append("..")

import asyncio
import re
from greencrawler import Crawler, CrawlingMode
from sqlalchemy import Table, Index, UniqueConstraint, Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy import select, insert


class EmailSeeker(Crawler):
    """Custom crawler."""
    email_table: Table

    def _define_db_tables(self):
        """Defines table to store email addresses."""
        self.email_table = Table(
            "emails",
            self.metadata_obj,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("token_id", ForeignKey("tokens.id")),
            Column("email", String(1023)),
            Column("url", String(1023)),
            UniqueConstraint("token_id", "email"),
            Index("idx_token", "token_id")
        )
        super()._define_db_tables()

    def _check_email_exists(self, email: str) -> bool:
        """Checks, if email already added."""
        with self.engine.connect() as connection:
            statement = (select(self.email_table)
                    .where(self.email_table.c.token_id == self.token_id)
                    .where(self.email_table.c.email == email)
                    .limit(1))
            email = connection.execute(statement).first()
        return bool(email)

    def _add_email(self, email: str, url: str) -> None:
        """Insert new email into email_table."""
        with self.engine.connect() as connection:
            connection.execute(
                insert(self.email_table).values(
                    token_id=self.token_id,
                    email=email,
                    url=url))
            connection.commit()

    def custom_process_url(self, url: str, html: str) -> None:
        """Process content of the page."""
        emails = re.findall(r'([\w.+-]+@[\w-]+(\.[\w-]+)*\.[a-zA-Z]{2,})', html)
        for email, _ in emails:
            email = email.lower()
            if not self._check_email_exists(email):
                self._add_email(email, url)


if __name__ == '__main__':
    crawler = EmailSeeker(number_of_tasks=10)
    asyncio.run(crawler.start(initial_url='https://halfdata.net/',
                              crawling_mode = CrawlingMode.DOMAIN_AND_SUBDOMAINS))
