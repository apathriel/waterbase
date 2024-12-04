import os
from typing import List, Tuple, TypedDict

from bs4 import BeautifulSoup
from DatabaseManager import CrawledLink, DatabaseManager
from dotenv import load_dotenv
from langchain_core.document_loaders import BaseLoader
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_openai import OpenAIEmbeddings
from langchain_postgres import PGVector
from langchain_text_splitters import (
    HTMLHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)
from tqdm import tqdm
from utils.logger_utils import get_logger


class ChunkingConfigurations(TypedDict):
    chunk_size: int
    chunk_overlap: int


class HTMLChunker:
    def __init__(
        self,
        db_url: str,
        pgvector_db_url: str,
        chunking_configurations: ChunkingConfigurations,
        headers_to_split_on: List[Tuple[str, str]] = None,
    ):
        self.logger = get_logger(self.__class__.__name__, "WARNING")
        self.embedding_model: Embeddings = OpenAIEmbeddings(
            model="text-embedding-3-small"
        )
        self.db_manager: DatabaseManager = DatabaseManager(connection_string=db_url)
        self.vector_store: PGVector = PGVector(
            connection=pgvector_db_url,
            embeddings=self.embedding_model,
            embedding_length=1536,
            collection_name="embeddings",
            use_jsonb=True,
            logger=self.logger,
        )
        self.documents: List[CrawledLink] = []
        self.headers_to_split_on = headers_to_split_on or [
            ("h1", "Header 1"),
            ("h2", "Header 2"),
            ("h3", "Header 3"),
        ]
        self.splitter: HTMLHeaderTextSplitter = HTMLHeaderTextSplitter(
            self.headers_to_split_on, return_each_element=True
        )
        self.loader: HTMLStringLoader = None
        self.recursive_splitter: RecursiveCharacterTextSplitter = (
            RecursiveCharacterTextSplitter(
                chunk_size=chunking_configurations["chunk_size"],
                chunk_overlap=chunking_configurations["chunk_overlap"],
            )
        )

    def fetch_documents(self, num_docs_to_fetch: int = None) -> List[CrawledLink]:
        return self.db_manager.fetch_links_with_content(
            CrawledLink, limit=num_docs_to_fetch, filters=[CrawledLink.allowed == True]
        )

    def load_document(self, sql_item: CrawledLink) -> Document:
        self.loader = HTMLStringLoader(sql_item)
        return self.loader.load()

    def run(self):
        self.documents = self.fetch_documents(num_docs_to_fetch=None)
        for doc in tqdm(self.documents, desc="Processing documents"):
            data = self.load_document(doc)
            # Split the document using HTMLHeaderTextSplitter
            header_chunks = self.splitter.split_text(data.page_content)
            # Append metadata to each chunk
            for chunk in header_chunks:
                chunk.metadata["source"] = data.metadata["source"]
                chunk.metadata["title"] = data.metadata["title"]
            # Split the chunks using RecursiveCharacterTextSplitter
            splits = self.recursive_splitter.split_documents(header_chunks)
            if splits:
                try:
                    document_ids = self.vector_store.add_documents(splits)
                except:
                    self.logger.error("Error adding documents to vector store")
                    continue
            self.logger.info(
                f"Successfully added {len(document_ids)} documents to vector store"
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
    load_dotenv()
    db_url = os.getenv("SQLALCHEMY_DATABASE_URL")
    vector_db_url = os.getenv("PGVECTOR_DATABASE_URL")
    chunking_config = ChunkingConfigurations(chunk_size=250, chunk_overlap=25)
    chunker = HTMLChunker(db_url, vector_db_url, chunking_config)
    chunker.run()


if __name__ == "__main__":
    main()
