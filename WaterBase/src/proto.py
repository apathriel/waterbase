import asyncio
import sys
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import aiohttp
import pandas as pd
from playwright.async_api import async_playwright
from utils.logger_utils import get_logger

logger = get_logger(__name__, "INFO")


class CustomRobotFileParser(RobotFileParser):
    def __init__(self, url):
        super().__init__(url)
        self.additional_disallowed_paths = []
        logger.debug(f"Created CustomRobotFileParser for {url}")

    def add_disallowed_path(self, path):
        """
        Add a path to be disallowed, mimicking robots.txt rules.
        """
        self.additional_disallowed_paths.append(path)

    def can_fetch(self, useragent, url):
        """
        Override to enforce additional disallowed paths.
        """
        parsed_url = urlparse(url).path
        for disallowed_path in self.additional_disallowed_paths:
            if parsed_url.startswith(disallowed_path):
                return False
        return super().can_fetch(useragent, url)


class WebCrawler:
    def __init__(self, base_url, max_depth=1, max_concurrent_tasks=10):
        self.base_url = base_url
        self.robot_parser = CustomRobotFileParser(base_url)
        self.visited_urls = set()
        self.found_links = []
        self.max_depth = max_depth
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.session = None

    async def __aenter__(self):
        """Create a single aiohttp session for multiple requests"""
        self.session = await aiohttp.ClientSession().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Properly close the aiohttp session"""
        await self.session.close()

    async def load_robots_txt(self):
        logger.debug(f"Fetching robots.txt from {self.base_url}...")
        robots_url = urljoin(self.base_url, "robots.txt")
        async with self.session.get(robots_url) as response:
            if response.status == 200:
                robots_txt = await response.text()
                logger.debug(f"The contents of robots.txt: {robots_txt}")
                self.robot_parser.parse(robots_txt.splitlines())
            else:
                logger.warning("Could not fetch robots.txt. Assuming no restrictions.")
        self.robot_parser.add_disallowed_path("/mediebank/")

    async def fetch_links(self, page, url):
        try:
            logger.info(f"Crawling: {url}")
            await page.goto(url)
            links = await page.eval_on_selector_all(
                "a[href]", "elements => elements.map(el => el.href)"
            )
            return set(urljoin(url, link) for link in links)
        except Exception as e:
            logger.error(f"Error fetching links from {url}: {e}")
            return set()

    async def _get_url_type(self, url):
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()

        # Use extensions for common types
        if any(path.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".bmp"]):
            return "image"
        if any(path.endswith(ext) for ext in [".pdf", ".doc", ".docx"]):
            return "document"
        if any(path.endswith(ext) for ext in [".mp4", ".avi", ".mov", ".mkv"]):
            return "video"
        if any(path.endswith(ext) for ext in [".mp3", ".wav", ".flac"]):
            return "audio"

        # Use a HEAD request if extension-based inference fails
        try:
            logger.warning(f"Could not determine type for {url}. Using HEAD request.")
            async with self.session.head(url, allow_redirects=True) as response:
                content_type = response.headers.get("Content-Type", "").lower()
                if "image" in content_type:
                    return "image"
                if "pdf" in content_type:
                    return "document"
                if "video" in content_type:
                    return "video"
                if "audio" in content_type:
                    return "audio"
                if "text/html" in content_type:
                    return "webpage"
        except Exception as e:
            logger.debug(f"Error determining type for {url}: {e}")
        return "unknown"

    async def crawl(self, url, context, depth=0):
        async with self.semaphore:
            if url in self.visited_urls or depth > self.max_depth:
                return
            self.visited_urls.add(url)
            is_allowed = self.robot_parser.can_fetch("*", url)

            # Determine URL type
            # url_type = await self._get_url_type(url)

            # Extract main endpoint
            parsed_url = urlparse(url)
            main_endpoint = parsed_url.path.lstrip("/").split("/")[0]

            # Save the result
            self.found_links.append(
                {
                    "url": url,
                    "allowed": is_allowed,
                    # "type": url_type,
                    "main_endpoint": main_endpoint,
                }
            )

            if not is_allowed:
                logger.info(f"Disallowed by robots.txt: {url}")
                return

            try:
                page = await context.new_page()
                links = await self.fetch_links(page, url)
                await page.close()

                for link in links:
                    if link.startswith(self.base_url) and link not in self.visited_urls:
                        await self.crawl(link, context, depth + 1)
            except Exception as e:
                logger.error(f"Error crawling {url}: {e}")

    def save_links_to_csv(self, filename):
        """
        Save results to CSV using Pandas.
        """
        logger.info(f"Saving results to {filename}...")
        df = pd.DataFrame(self.found_links)
        df.to_csv(filename, index=False)

    async def run(self):
        try:
            await self.load_robots_txt()
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                context = await browser.new_context()

                await self.crawl(self.base_url, context)

                await browser.close()
                logger.info(f"Crawl completed. Found {len(self.found_links)} links.")
        except KeyboardInterrupt:
            logger.warning("Crawl interrupted. Saving progress...")
        finally:
            self.save_links_to_csv("crawled_links.csv")


async def main():
    base_url = "https://www.aarhusvand.dk"
    async with WebCrawler(base_url, max_depth=1) as crawler:
        await crawler.run()


if __name__ == "__main__":
    asyncio.run(main())
