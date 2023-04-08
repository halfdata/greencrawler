"""Simple example how to use greencrawler."""
import asyncio
from greencrawler import Crawler, CrawlingMode


class MyCrawler(Crawler):
    """Custom crawler."""


if __name__ == '__main__':
    cr = MyCrawler(
        initial_url='https://halfdata.net/',
        crawling_mode = CrawlingMode.DOMAIN_AND_SUBDOMAINS,
        number_of_tasks=10)

    cr.set_forbidden_keywords([r'wp-json', r'\/feed'])
    asyncio.run(cr.start())
