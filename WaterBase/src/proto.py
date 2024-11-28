import asyncio
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import aiohttp
import pandas as pd
from playwright.async_api import async_playwright


class CustomRobotFileParser(RobotFileParser):
    def __init__(self, url):
        super().__init__(url)
        self.additional_disallowed_paths = []

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
    def __init__(self, base_url):
        self.base_url = base_url
        self.robot_parser = CustomRobotFileParser(base_url)
        self.visited_urls = set()
        self.found_links = []

    async def load_robots_txt(self):
        robots_url = urljoin(self.base_url, "robots.txt")
        async with aiohttp.ClientSession() as session:
            async with session.get(robots_url) as response:
                if response.status == 200:
                    robots_txt = await response.text()
                    self.robot_parser.parse(robots_txt.splitlines())
                else:
                    print("Could not fetch robots.txt. Assuming no restrictions.")
        self.robot_parser.add_disallowed_path("/mediebank/")

    async def fetch_links(self, page, url):
        print(f"Crawling: {url}")
        await page.goto(url)
        links = await page.eval_on_selector_all(
            "a[href]", "elements => elements.map(el => el.href)"
        )
        return set(urljoin(url, link) for link in links)

    async def get_url_type(self, url):
        """
        Identify the type of the URL based on its structure or Content-Type header.
        """
        parsed_url = urlparse(url)
        path = parsed_url.path

        # Check for file extensions
        if path.endswith((".jpg", ".jpeg", ".png", ".gif", ".bmp")):
            return "image"
        elif path.endswith((".pdf", ".doc", ".docx")):
            return "document"
        elif path.endswith((".mp4", ".avi", ".mov", ".mkv")):
            return "video"
        elif path.endswith((".mp3", ".wav", ".flac")):
            return "audio"

        # Use HEAD request for Content-Type
        async with aiohttp.ClientSession() as session:
            try:
                async with session.head(url, allow_redirects=True) as response:
                    content_type = response.headers.get("Content-Type", "").lower()
                    if "image" in content_type:
                        return "image"
                    elif "pdf" in content_type:
                        return "document"
                    elif "video" in content_type:
                        return "video"
                    elif "audio" in content_type:
                        return "audio"
                    elif "text/html" in content_type:
                        return "webpage"
            except Exception:
                pass
        return "unknown"

    async def crawl(self, url, context):
        if url in self.visited_urls:
            return
        self.visited_urls.add(url)
        is_allowed = self.robot_parser.can_fetch("*", url)

        # Determine URL type
        url_type = await self.get_url_type(url)

        # Save the result
        self.found_links.append({"url": url, "allowed": is_allowed, "type": url_type})

        if not is_allowed:
            print(f"Disallowed by robots.txt: {url}")
            return

        try:
            page = await context.new_page()
            links = await self.fetch_links(page, url)
            await page.close()

            for link in links:
                if link.startswith(self.base_url) and link not in self.visited_urls:
                    await self.crawl(link, context)
        except Exception as e:
            print(f"Error crawling {url}: {e}")

    def save_links_to_csv(self, filename):
        """
        Save results to CSV using Pandas.
        """
        print(f"Saving results to {filename}...")
        df = pd.DataFrame(self.found_links)
        df.to_csv(filename, index=False)

    async def run(self):
        await self.load_robots_txt()

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context()

            await self.crawl(self.base_url, context)

            await browser.close()

        self.save_links_to_csv("crawled_links.csv")


async def main():
    base_url = "https://www.aarhusvand.dk"
    crawler = WebCrawler(base_url)
    await crawler.run()


if __name__ == "__main__":
    asyncio.run(main())
