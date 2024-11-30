import os
from datetime import datetime
from typing import Dict, List

from sqlalchemy import Boolean, Column, DateTime, String, Text, create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.logging_utils import get_logger

Base = declarative_base()


class CrawledLink(Base):
    __tablename__ = "crawled_links"

    url = Column(String, primary_key=True)
    allowed = Column(Boolean)
    type = Column(String)
    inferred_type = Column(String)
    main_endpoint = Column(String)
    title = Column(Text)
    description = Column(Text)
    pageID = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DatabaseManager:
    def __init__(self, connection_string=None, batch_size=64):
        self.engine = create_engine(connection_string or os.getenv("DATABASE_URL"))
        self.Session = sessionmaker(bind=self.engine)
        self.session_instance = None
        self.connection = None
        self.logger = get_logger(self.__class__.__name__, "INFO")
        self.batch_size = batch_size
        self._batch = []
        Base.metadata.create_all(self.engine)
        self.connect_to_database()

    def connect_to_database(self):
        try:
            self.session_instance = self.Session()
            self.connection = self.engine.connect()
            self.logger.info("Database connection established")
        except Exception as e:
            self.logger.error(f"Database connection error: {e}")
            raise

    async def add_crawled_link(self, data: Dict):
        self._batch.append(data)
        if len(self._batch) >= self.batch_size:
            await self.flush_batch()

    async def flush_batch(self):
        if not self._batch:
            return

        try:
            insert_stmt = insert(CrawledLink).values(self._batch)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                constraint=CrawledLink.__table__.primary_key,
                set_={
                    "allowed": insert_stmt.excluded.allowed,
                    "type": insert_stmt.excluded.type,
                    "inferred_type": insert_stmt.excluded.inferred_type,
                    "main_endpoint": insert_stmt.excluded.main_endpoint,
                    "title": insert_stmt.excluded.title,
                    "description": insert_stmt.excluded.description,
                    "pageID": insert_stmt.excluded.pageID,
                    "updated_at": datetime.utcnow(),
                },
            )

            self.connection.execute(upsert_stmt)
            self.connection.commit()
            self.logger.debug(f"Flushed batch of {len(self._batch)} records")
            self._batch.clear()
        except Exception as e:
            self.logger.error(f"Error flushing batch: {e}")
            self._batch.clear()
            raise

    def close_database_connection(self):
        try:
            if self._batch:
                self.flush_batch()
            if self.connection:
                self.connection.close()
            if self.session_instance:
                self.session_instance.close()
            self.logger.info("Database connections closed")
        except Exception as e:
            self.logger.error(f"Error closing database connections: {e}")
