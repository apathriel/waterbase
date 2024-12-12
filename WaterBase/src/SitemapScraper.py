import json
import random
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Optional

import requests
from playwright.sync_api import Page, sync_playwright
from tqdm import tqdm

from .utils.logger_utils import get_logger

logger = get_logger(__name__, "INFO")


class SitemapScraper:
    def __init__(self, headless: bool = False):
        self.headless = headless
        self._urls: List[str] = []

    @property
    def urls(self) -> List[str]:
        return self._urls

    @urls.setter
    def urls(self, new_urls: List[str]) -> None:
        if not isinstance(new_urls, list):
            raise TypeError("URLs must be provided as a list")

        # Validate all elements are non-empty strings
        if not all(isinstance(url, str) and url.strip() for url in new_urls):
            raise ValueError("All URLs must be non-empty strings")

        # Store cleaned URLs
        self._urls = [url.strip() for url in new_urls]
        logger.info(f"Updated URLs list with {len(self._urls)} items")

    def sample_urls(self, n: int = 1) -> List[str]:
        if not self._urls:
            logger.warning("No URLs available to sample from")
            return []

        try:
            n = int(n)
            if n < 1:
                raise ValueError("Sample size must be positive")
            if n > len(self._urls):
                logger.warning(
                    f"Requested sample size {n} exceeds available URLs {len(self._urls)}"
                )
                n = len(self._urls)

            sampled = random.sample(self._urls, n)
            logger.debug(f"Sampled {len(sampled)} URLs: {sampled}")
            return sampled

        except (TypeError, ValueError) as e:
            logger.error(f"Invalid sample size: {e}")
            return []

    @staticmethod
    def fetch_sitemap_by_url(url: str) -> Optional[str]:
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        sitemap_url = f"{url.rstrip('/')}/sitemap.xml"
        logger.info(f"Fetching sitemap from: {sitemap_url}")
        try:
            response = requests.get(sitemap_url)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching sitemap: {e}")
            return None

    @staticmethod
    def fetch_sitemap_from_file(file_path: str) -> Optional[str]:
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                return file.read()  # Read entire file as single string
        except IOError as e:
            logger.error(f"Error reading sitemap file: {e}")
            return None

    def parse_sitemap(
        self, sitemap_url: Optional[str] = None, sitemap_file_path: Optional[str] = None
    ) -> Optional[List[str]]:
        sitemap = None  # Initialize sitemap to None
        if sitemap_url:
            sitemap = self.fetch_sitemap_by_url(sitemap_url)
        elif sitemap_file_path:
            sitemap = self.fetch_sitemap_from_file(sitemap_file_path)
        if sitemap:
            root = ET.fromstring(sitemap)

            # Handle different sitemap namespaces
            namespaces = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            extracted_urls = [
                url.text
                for url in root.findall(".//ns:url/ns:loc", namespaces)
                if url.text is not None
            ]
            # Extract URLs from sitemap
            self.urls = extracted_urls

            logger.info(f"Found {len(self._urls)} URLs in sitemap")
            logger.debug(f"URLs: {self._urls}")
            return self.urls
        else:
            logger.error("No sitemap found")
            return None

    def scrape_url(self, page: Page, url: str):
        try:
            page.goto(url)
            page.wait_for_load_state("networkidle")

            content = {
                "url": page.url,
                "title": page.evaluate('() => document.querySelector("h1")?.innerText'),
                "description": page.evaluate(
                    '() => document.querySelector("meta[name=description]")?.content'
                ),
                "main_content": page.evaluate(
                    """() => {
                    const content = [];
                    // Get text from article sections
                    document.querySelectorAll('article, section').forEach(section => {
                        const sectionText = section.innerText.trim();
                        if(sectionText) content.push(sectionText);
                    });
                    return content.join('\\n\\n');
                }"""
                ),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

            return content

        except Exception as e:
            logger.error(f"Error scraping URL: {url}")
            return {
                "url": url,
                "error": str(e),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

    def scrape_sitemap(self, output_file: str = "out/sitemap.json"):
        output_path = Path(output_file)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        scraped_content = []

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context()
            page = context.new_page()

            for url in tqdm(self._urls, desc="Scraping URLs", unit="page"):
                content = self.scrape_url(page, url)
                scraped_content.append(content)

                with open(output_file, "w", encoding="utf-8") as file:
                    json.dump(scraped_content, file, indent=2, ensure_ascii=False)

                time.sleep(1)  # Sleep for 1 second between requests

            context.close()
            browser.close()

        return scraped_content


def main():
    scraper = SitemapScraper()
    scraper.parse_sitemap(sitemap_file_path="in/sitemap.xml")
    scraper.scrape_sitemap()


if __name__ == "__main__":
    main()
