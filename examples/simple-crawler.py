"""Simple example how to use greencrawler."""
import sys
sys.path.append("..")

import asyncio
from greencrawler import Crawler, CrawlingMode


class MyCrawler(Crawler):
    """Custom crawler."""


if __name__ == '__main__':
    crawler = MyCrawler(number_of_tasks=10, urls_limit=100)
    asyncio.run(crawler.start(initial_url='https://halfdata.net/'))
