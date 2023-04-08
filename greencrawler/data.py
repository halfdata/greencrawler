"""Define database tables."""
from sqlalchemy import MetaData
from sqlalchemy import Table, Index, UniqueConstraint, Column, ForeignKey
from sqlalchemy import Integer, String, Enum, Boolean, DateTime

from .classes import CrawlingMode

metadata_obj = MetaData()

token_table = Table(
    "tokens",
    metadata_obj,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("url", String(1023)),
    Column("mode", Enum(CrawlingMode), default=CrawlingMode.ALL),
    Column("created", DateTime)
)

url_table = Table(
    "urls",
    metadata_obj,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("hash_id", String(63)),
    Column("token_id", ForeignKey("tokens.id")),
    Column("url", String(1023)),
    Column("status", Integer, nullable=True),
    Column("fetched", Boolean, default=False),
    Column("processed", Boolean, default=False),
    UniqueConstraint("token_id", "hash_id"),
    Index("idx_token_hash", "token_id", "hash_id")
)
