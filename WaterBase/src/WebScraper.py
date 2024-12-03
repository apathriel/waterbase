import os
from pathlib import Path

from bs4 import BeautifulSoup, Comment
from DatabaseManager import CrawledLink, DatabaseManager
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from utils.logger_utils import get_logger


class HTMLParser:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__, "INFO")

    def extract_html_metadata(self, html_content, url=None):
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            title = soup.title.string.strip() if soup.title else "No Title"
            author = soup.find("meta", {"name": "author"})
            author = author["content"].strip() if author else "Unknown"
            publication_date = soup.find("meta", {"name": "publication_date"})
            publication_date = (
                publication_date["content"].strip() if publication_date else "Unknown"
            )
            description = soup.find("meta", {"name": "description"})
            description = (
                description["content"].strip() if description else "No Description"
            )
            keywords = soup.find("meta", {"name": "keywords"})
            keywords = keywords["content"].strip() if keywords else "No Keywords"
            language = soup.find("html")["lang"] if soup.find("html") else "Unknown"

            metadata = {
                "title": title,
                "author": author,
                "publication_date": publication_date,
                "description": description,
                "keywords": keywords,
                "url": url,
                "language": language,
            }

            # Create <meta> tags
            meta_tags = f"""
            <head>
                <meta name="title" content="{title}">
                <meta name="author" content="{author}">
                <meta name="publication_date" content="{publication_date}">
                <meta name="description" content="{description}">
                <meta name="keywords" content="{keywords}">
                <meta name="url" content="{url}">
                <meta name="language" content="{language}">
            </head>
            """

            return meta_tags
        except Exception as e:
            return f"<head><meta name='error' content='An error occurred: {e}'></head>"

    def process_divs_recursively(self, soup):
        for div in soup.find_all("div"):
            # Recursively process child divs first
            self.process_divs_recursively(div)

            if not div.get_text(strip=True):
                div.decompose()

    def flatten_single_child_divs(self, soup):
        for div in soup.find_all("div"):
            children = div.find_all(recursive=False)
            if len(children) == 1:
                div.unwrap()

    def remove_all_attributes(self, soup):
        for tag in soup.find_all(True):  # Process all tags
            tag.attrs = {}  # Clear all attributes

    def remove_empty_text_nodes(soup):
        for element in soup.find_all(string=True):
            if not element.strip():
                element.extract()

    def extract_html_content(
        self, html_content, tags_to_remove=None, unwrap_divs: bool = False
    ):
        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # Remove HTML comments
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                comment.extract()

            # Define tags to remove if not provided
            if tags_to_remove is None:
                tags_to_remove = [
                    "script",
                    "style",
                    "header",
                    "footer",
                    "nav",
                    "avds-newsletter-signup",
                    "iframe",
                    "link",
                    "svg",
                    "figcaption",
                    "a",
                    "img",
                    "picture",
                    "video",
                    "br",
                    "audio",
                    "avds-video-embed",
                    "avds-slider",
                    "button",
                    "input",
                    "article",
                    "avds-scroll-to-anchor",
                    "avds-tab",
                    "avds-tab-panel",
                ]

            # Remove specified tags
            for tag in soup(tags_to_remove):
                tag.decompose()

            # Optional: Remove specific <div> or elements by ID or attribute
            cookie_div = soup.find("div", id="cookie-information-template-wrapper")
            if cookie_div:
                cookie_div.decompose()

            raffle_button = soup.find(
                "button", {"data-testid": "raffle-search-launcher-button"}
            )
            if raffle_button:
                raffle_button.decompose()

            raffle_div = soup.find("div", {"data-testid": "raffle-search-launcher"})
            if raffle_div:
                raffle_div.decompose()

            self.process_divs_recursively(soup)
            self.flatten_single_child_divs(soup)
            self.remove_all_attributes(soup)

            content = ""
            for element in soup.find_all(
                ["body"],
                recursive=True,
            ):
                if element.name in {"ul", "ol"}:
                    for li in element.find_all("li"):
                        if not li.get_text(strip=True):
                            try:
                                li.decompose()
                            except Exception as e:
                                self.logger.error(f"Error removing empty <li>: {e}")
                    if all(
                        not child.get_text(strip=True) for child in element.contents
                    ):
                        continue

                content += str(element)

            return str(BeautifulSoup(str(content), "html.parser"))

        except Exception as e:
            self.logger.error(f"Error extracting content: {e}")
            return {"title": "Error", "content": f"An error occurred: {e}"}


class WebScraper:
    def __init__(self, db_url):
        self.db_manager = DatabaseManager(connection_string=db_url)
        self.logger = get_logger(self.__class__.__name__, "INFO")
        self.links_to_scrape = []
        self.html_parser = HTMLParser()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def load_links_to_scrape(self, limit=5):
        self.links_to_scrape = self.db_manager.fetch_links_without_content(limit)

    def load_endpoint_sample_to_scrape(self, limit=3, output_dir="out"):
        sampled_links = self.db_manager.sample_records_by_group(
            model=CrawledLink,
            group_attr="main_endpoint",
            sample_size=limit,
            filters=[CrawledLink.allowed == True],
        )

        for endpoint, links in sampled_links.items():
            Path(f"{output_dir}/{endpoint}").mkdir(parents=True, exist_ok=True)
            for link in links:
                content_data = self.extract_content(link.url)
                self.logger.info(f"Scraping: {link.url}")
                if content_data:
                    file_path = Path(f"{output_dir}/{endpoint}/{link.pageID}.html")
                    with open(file_path, "w", encoding="utf-8") as file:
                        file.write(content_data)

    def extract_content(self, url):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url)
                content = page.content()
                browser.close()
                return self.html_parser.extract_html_content(content)
        except Exception as e:
            self.logger.error(f"Error extracting content from {url}: {e}")
            return {"title": "", "content": ""}

    def run(self, limit=None):
        self.load_links_to_scrape(limit)
        for link in self.links_to_scrape:
            self.logger.info(f"Scraping: {link.url}")
            content_data = self.extract_content(link.url)
            # Update the database entry with the extracted content
            self.db_manager.update_crawled_link(
                link.url,
                {
                    "title": content_data.get("title") or link.title,
                    "content": content_data.get("content"),
                },
            )


def main():
    load_dotenv()
    db_url = os.getenv("SQLALCHEMY_DATABASE_URL")
    with WebScraper(db_url) as scraper:
        scraper.load_endpoint_sample_to_scrape()
        # scraper.run(limit=5)

    """ with WebScraper(db_url) as scraper:
        scraper.load_links_to_scrape(limit=5)
        print(scraper.links_to_scrape[3])
        print(scraper.extract_content(scraper.links_to_scrape[3].url)) """


if __name__ == "__main__":
    main()
