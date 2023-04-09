"""Simple example how to use greencrawler."""
import asyncio
from greencrawler import Crawler, CrawlingMode


class MyCrawler(Crawler):
    """Custom crawler."""


if __name__ == '__main__':
    crawler = MyCrawler(number_of_tasks=10)

    crawler.set_forbidden_keywords([r'wp-json', r'\/feed'])

    asyncio.run(crawler.start(initial_url='https://geosocks.com/',
                              crawling_mode = CrawlingMode.DOMAIN_AND_SUBDOMAINS))
