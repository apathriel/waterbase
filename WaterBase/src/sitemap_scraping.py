import json
from pathlib import Path
import random
import time
from typing import List
import xml.etree.ElementTree as ET

from playwright.sync_api import sync_playwright, Page
import requests
from tqdm import tqdm

from utils.logger_utils import get_logger

logger = get_logger(__name__, "INFO")


class ContentAnalyzer:
    def __init__(self, headless: bool = True):
        self.headless = headless

    def analyze_page(self):
        pass


class SitemapScraper:
    def __init__(self, headless: bool = False):
        self.headless = headless
        self._urls: List[str] = []

    @property
    def urls(self) -> List[str]:
        return self._urls

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
    def fetch_sitemap_by_url(url: str) -> List[str]:
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
    def fetch_sitemap_from_file(file_path: str) -> str:
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                return file.read()  # Read entire file as single string
        except IOError as e:
            logger.error(f"Error reading sitemap file: {e}")
            return None

    def parse_sitemap(
        self, sitemap_url: str = None, sitemap_file_path: str = None
    ) -> List[str]:
        if sitemap_url:
            sitemap = self.fetch_sitemap_by_url(sitemap_url)
        elif sitemap_file_path:
            sitemap = self.fetch_sitemap_from_file(sitemap_file_path)
        if sitemap:
            root = ET.fromstring(sitemap)

            # Handle different sitemap namespaces
            namespaces = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            # Extract URLs from sitemap
            self._urls = [
                url.text for url in root.findall(".//ns:url/ns:loc", namespaces)
            ]
            logger.info(f"Found {len(self._urls)} URLs in sitemap")
            logger.debug(f"URLs: {self._urls}")
            return self._urls
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
                "headings": page.evaluate(
                    """() => {
                    const headings = [];
                    document.querySelectorAll('h1, h2, h3').forEach(h => {
                        headings.push({
                            level: parseInt(h.tagName[1]),
                            text: h.innerText.trim()
                        });
                    });
                    return headings;
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
