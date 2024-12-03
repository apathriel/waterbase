import os

from bs4 import BeautifulSoup
from DatabaseManager import CrawledLink, DatabaseManager
from playwright.sync_api import sync_playwright
from utils.logger_utils import get_logger


class HTMLParser:
    def __init__(self):
        pass

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

    def extract_html_content(self, html_content, tags_to_remove=None):
        try:
            soup = BeautifulSoup(html_content, "html.parser")

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
                ]

            # Remove specified tags
            for tag in soup(tags_to_remove):
                tag.decompose()

            # Remove specific <div> by ID
            cookie_div = soup.find("div", id="cookie-information-template-wrapper")
            if cookie_div:
                cookie_div.decompose()

            raffle_button = soup.find(
                "button", {"data-testid": "raffle-search-launcher-button"}
            )
            if raffle_button:
                raffle_button.decompose()

            # Remove specific <div> by data-testid
            raffle_div = soup.find("div", {"data-testid": "raffle-search-launcher"})
            if raffle_div:
                raffle_div.decompose()

            # Extract the title
            title = (
                soup.title.string.strip()
                if soup.title and isinstance(soup.title.string, str)
                else "No Title"
            )

            # Extract the cleaned HTML content
            content = ""
            for element in soup.find_all(
                [
                    "h1",
                    "h2",
                    "h3",
                    "h4",
                    "h5",
                    "h6",
                    "p",
                    "ul",
                    "ol",
                    "li",
                    "a",
                    "blockquote",
                ]
            ):
                if "class" in element.attrs:
                    del element.attrs["class"]
                content += str(element)

            # Wrap the content in <body> tags
            body_content = f"<body>{content}</body>"

            with open("output.html", "w") as f:
                f.write(body_content)

            return {"title": title, "content": body_content}

        except Exception as e:
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
    db_url = os.getenv("SQLALCHEMY_DATABASE_URL")
    with WebScraper(db_url) as scraper:
        scraper.load_links_to_scrape(limit=5)
        print(scraper.links_to_scrape[3])
        print(scraper.extract_content(scraper.links_to_scrape[3].url))


if __name__ == "__main__":
    main()
