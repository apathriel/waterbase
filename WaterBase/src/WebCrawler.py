import asyncio
import os
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import aiohttp
from DatabaseManager import CrawledLink, DatabaseManager
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm
from url_normalize import url_normalize
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
    def __init__(
        self,
        base_url,
        max_depth=1,
        max_concurrent_tasks=50,
        user_agent=None,
        db_url=None,
    ):
        self.base_url = base_url
        self.max_depth = max_depth
        self.user_agent = user_agent or "*"

        self.robot_parser = CustomRobotFileParser(base_url)
        self.visited_urls = set()
        self.found_links_data = []
        self.urls_per_depth = {}

        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.session = None
        self.progress_bar = None
        self.queue = asyncio.Queue()

        self.db_manager = DatabaseManager(connection_string=db_url)

    async def __aenter__(self):
        """Create a single aiohttp session for multiple requests"""
        self.session = await aiohttp.ClientSession(
            headers={"User-Agent": self.user_agent}
        ).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Properly close the aiohttp session and database connection"""
        if exc_type is not None:
            # Handle any exceptions that occurred
            logger.error(f"Error occurred: {exc}")
            await self.db_manager.rollback()  # Add rollback on error
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
            logger.debug(f"Crawling: {url}")
            await page.goto(url)
            await page.wait_for_load_state("networkidle")

            # Extract metadata
            metadata = await self.extract_metadata(page)

            # Extract links
            links = await page.evaluate(
                """
                [...document.querySelectorAll('a[href]')].map(el => el.href)
            """
            )
            absolute_links = set(
                url_normalize(urljoin(url, link)) for link in links if "#" not in link
            )
            logger.debug(f"Found {len(absolute_links)} links on {url}")

            return absolute_links, metadata
        except Exception as e:
            logger.error(f"Error fetching links from {url}: {e}")
            return set(), {"title": "", "description": "", "pageID": ""}

    async def _get_url_type(self, url, fallback_to_head=False):
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()

        # Use extensions for common types
        if any(path.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".bmp"]):
            return "image"
        if any(path.endswith(ext) for ext in [".pdf", ".doc", ".docx", ".xlsx"]):
            return "document"
        if any(path.endswith(ext) for ext in [".mp4", ".avi", ".mov", ".mkv"]):
            return "video"
        if any(path.endswith(ext) for ext in [".mp3", ".wav", ".flac"]):
            return "audio"

        # Use a HEAD request if extension-based inference fails
        if fallback_to_head:
            try:
                logger.warning(
                    f"Could not determine type for {url}. Using HEAD request."
                )
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
        else:
            return "unknown"

    async def extract_metadata(self, page):
        """Extract metadata from page including OpenGraph tags"""
        metadata = await page.evaluate(
            """
            () => {
                const getMetaContent = (selectors) => {
                    for (const selector of selectors) {
                        const meta = document.querySelector(selector);
                        if (meta) {
                            const content = meta.getAttribute('content');
                            if (content) return content;
                        }
                    }
                    return '';
                };

                return {
                    title: getMetaContent(['meta[name="title"]', 'meta[property="og:title"]']) || document.title,
                    description: getMetaContent(['meta[name="description"]', 'meta[property="og:description"]']),
                    pageID: getMetaContent(['meta[name="pageID"]']),
                    type: getMetaContent(['meta[property="og:type"]', 'meta[name="type"]']) || 'webpage'
                };
            }
            """
        )

        return metadata

    def remove_query_params(self, url, params_to_remove):
        parsed = urlparse(url)
        query_params = dict(parse_qsl(parsed.query))
        filtered_query = {
            k: v for k, v in query_params.items() if k not in params_to_remove
        }
        new_query = urlencode(filtered_query)
        return urlunparse(parsed._replace(query=new_query))

    async def crawl(self, url, context, depth=0):
        await self.queue.put((url, depth))
        while not self.queue.empty():
            current_url, current_depth = await self.queue.get()
            filtered_url = self.remove_query_params(current_url, ["tags"])
            normalized_url = url_normalize(filtered_url)
            if normalized_url in self.visited_urls or current_depth > self.max_depth:
                continue
            self.visited_urls.add(normalized_url)
            self.urls_per_depth[current_depth] = (
                self.urls_per_depth.get(current_depth, 0) + 1
            )

            is_allowed = self.robot_parser.can_fetch("*", normalized_url)
            url_type = await self._get_url_type(current_url)
            parsed_url = urlparse(current_url)
            main_endpoint = parsed_url.path.lstrip("/").split("/")[0]

            try:
                page = await context.new_page()
                links, metadata = await self.fetch_links(page, current_url)
                await page.close()

                # Create CrawledLink compatible dictionary
                link_data = {
                    "url": normalized_url,  # Use normalized URL as primary key
                    "allowed": is_allowed,
                    "type": metadata.get("type", "webpage"),
                    "inferred_type": url_type,
                    "main_endpoint": main_endpoint,
                    "title": metadata.get("title", ""),
                    "description": metadata.get("description", ""),
                    "pageID": metadata.get("pageID", ""),
                }
                await self.db_manager.add_crawled_link(link_data)

                for link in links:
                    if link.startswith(self.base_url) and link not in self.visited_urls:
                        await self.queue.put((link, current_depth + 1))
            except Exception as e:
                logger.error(f"Error crawling {current_url}: {e}")
            finally:
                if len(self.visited_urls) % 10 == 0:
                    self.progress_bar.update(10)

    async def run(self):
        try:
            await self.load_robots_txt()
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                context = await browser.new_context(user_agent=self.user_agent)

                self.progress_bar = tqdm(total=self.max_depth, desc="Crawling Progress")
                await self.crawl(self.base_url, context)
                self.progress_bar.close()

                # Ensure final batch is saved
                await self.db_manager.flush_batch()
                await browser.close()

                logger.info(f"Crawl completed. Found {len(self.visited_urls)} links.")
                logger.info(f"URLs crawled per depth: {self.urls_per_depth}")
        except KeyboardInterrupt:
            logger.warning("Crawl interrupted. Saving final batch...")
            await self.db_manager.flush_batch()
        finally:
            self.db_manager.close_database_connection()


async def main():
    load_dotenv()

    base_url = os.getenv("BASE_URL")
    database_url = os.getenv("SQLALCHEMY_DATABASE_URL")
    async with WebCrawler(base_url, max_depth=4, db_url=database_url) as crawler:
        await crawler.run()


if __name__ == "__main__":
    asyncio.run(main())
