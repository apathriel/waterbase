import os

from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_postgres import PGVector


def main():
    load_dotenv()
    pgvector_db_url = os.getenv("PGVECTOR_DATABASE_URL")

    vector_store: PGVector = PGVector(
        connection=pgvector_db_url,
        embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
        embedding_length=1536,
        collection_name="embeddings_raw_750_50",
        use_jsonb=True,
    )
    retriever = vector_store.as_retriever()


if __name__ == "__main__":
    main()
