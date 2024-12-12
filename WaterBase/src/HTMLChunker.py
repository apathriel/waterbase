import os
import re
import tempfile
from typing import List, Tuple, TypedDict

from bs4 import BeautifulSoup
from DatabaseManager import CrawledLink, DatabaseManager
from dotenv import load_dotenv
from langchain_community.document_loaders import BSHTMLLoader
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
        self.logger = get_logger(self.__class__.__name__, "Info")
        self.embedding_model: Embeddings = OpenAIEmbeddings(
            model="text-embedding-3-small"
        )
        self.db_manager: DatabaseManager = DatabaseManager(connection_string=db_url)
        self.vector_store: PGVector = PGVector(
            connection=pgvector_db_url,
            embeddings=self.embedding_model,
            embedding_length=1536,
            pre_delete_collection=True,
            collection_name="embeddings_raw_750_50",
            use_jsonb=True,
            logger=self.logger,
        )
        self.documents: List[CrawledLink] = []
        self.headers_to_split_on = headers_to_split_on or [
            ("h1", "Header 1"),
            ("avds-accordion", "Folder"),
        ]
        self.splitter: HTMLHeaderTextSplitter = HTMLHeaderTextSplitter(
            self.headers_to_split_on, return_each_element=True
        )
        self.html_loader: HTMLStringLoader = None
        self.bs_loader: BSHTMLLoader = None
        self.recursive_splitter: RecursiveCharacterTextSplitter = (
            RecursiveCharacterTextSplitter(
                chunk_size=chunking_configurations["chunk_size"],
                chunk_overlap=chunking_configurations["chunk_overlap"],
                separators=[".", "\n"],
            )
        )

    def fetch_documents(self, num_docs_to_fetch: int = None) -> List[CrawledLink]:
        return self.db_manager.fetch_links_with_content(
            CrawledLink, limit=num_docs_to_fetch, filters=[CrawledLink.allowed == True]
        )

    def load_document_html(self, sql_item: CrawledLink) -> Document:
        self.html_loader = HTMLStringLoader(sql_item)
        return self.html_loader.load()

    def generate_embeddings_from_raw_html_content(self):
        self.documents = self.fetch_documents(num_docs_to_fetch=None)
        for doc in tqdm(self.documents, desc="Processing documents"):
            with tempfile.NamedTemporaryFile("w", delete=False) as temp_file:
                temp_file.write(doc.site_content)
                temp_file_path = temp_file.name
            # Load the document using BSHTMLLoader
            self.bs_loader = BSHTMLLoader(temp_file_path)
            data = self.bs_loader.load()
            # Remove multiple newlines
            data[0].page_content = re.sub(r"\n{2,}", " ", data[0].page_content).strip()
            # Update source to use url from CrawledLink row in database
            data[0].metadata["source"] = doc.url
            # Split the document using RecursiveCharacterTextSplitter
            splits = self.recursive_splitter.split_documents(data)
            if splits:
                try:
                    document_ids = self.vector_store.add_documents(splits)
                except:
                    self.logger.error("Error adding documents to vector store")
                    continue
            self.logger.info(
                f"Successfully added {len(document_ids)} documents to vector store"
            )

    def generate_embeddings_by_html_header(self):
        self.documents = self.fetch_documents(num_docs_to_fetch=None)
        for doc in tqdm(self.documents, desc="Processing documents"):
            data = self.load_document_html(doc)
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
    chunking_config = ChunkingConfigurations(chunk_size=800, chunk_overlap=60)
    chunker = HTMLChunker(db_url, vector_db_url, chunking_config)
    chunker.generate_embeddings_from_raw_html_content()


if __name__ == "__main__":
    main()
