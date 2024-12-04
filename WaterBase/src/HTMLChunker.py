import os
from email.mime import base
from importlib import metadata

from bs4 import BeautifulSoup
from DatabaseManager import CrawledLink, DatabaseManager
from dotenv import load_dotenv
from langchain_community.document_loaders import BSHTMLLoader
from langchain_core.document_loaders import BaseLoader
from langchain_core.documents import Document
from langchain_text_splitters import (
    HTMLHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)


class HTMLChunker:
    def __init__(self, db_url: str):
        self.db_manager = DatabaseManager(connection_string=db_url)

    def proto(self):
        return self.db_manager.fetch_links_with_content(
            CrawledLink, limit=1, filters=[CrawledLink.allowed == True]
        )


class HTMLStringLoader(BaseLoader):
    def __init__(self, sql_item: CrawledLink):
        self.html_string = sql_item.site_content
        self.url = sql_item.url
        self.title = sql_item.title

    def load(self):
        soup = BeautifulSoup(self.html_string, "html.parser")
        return Document(
            page_content=str(soup), metadata={"source": self.url, "title": self.title}
        )


def main():
    headers_to_split_on = [
        ("h1", "Header 1"),
        ("h2", "Header 2"),
        ("h3", "Header 3"),
    ]
    load_dotenv()
    db_url = os.getenv("SQLALCHEMY_DATABASE_URL")
    chunker = HTMLChunker(db_url)
    html_splitter = HTMLHeaderTextSplitter(
        headers_to_split_on, return_each_element=True
    )
    split = chunker.proto()
    custom_loader = HTMLStringLoader(split[0])
    data = custom_loader.load()
    chunks = html_splitter.split_text(data.page_content)
    recursive_splitter = RecursiveCharacterTextSplitter(
        chunk_size=100, chunk_overlap=20, length_function=len, is_separator_regex=False
    )
    bs_loader = BSHTMLLoader("out/cases/1565.html")
    docs = bs_loader.load()
    custom_texts = recursive_splitter.split_documents([data])
    base_texts = recursive_splitter.split_documents(docs)

    print(len(custom_texts))
    print(len(base_texts))

    print(custom_texts[0])
    print(base_texts[0])


if __name__ == "__main__":
    main()
