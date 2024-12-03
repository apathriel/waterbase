import os
import random
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Type

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    String,
    Text,
    create_engine,
    or_,
    select,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.logger_utils import get_logger

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
    site_content = Column(Text)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self):
        return (
            f"<CrawledLink(url={self.url}, allowed={self.allowed}, type={self.type}, "
            f"inferred_type={self.inferred_type}, main_endpoint={self.main_endpoint}, "
            f"title={self.title}, description={self.description}, pageID={self.pageID}, "
            f"site_content={self.site_content[:30]}...)>"
        )


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

    def fetch_links_without_content(self, limit=None):
        with self.Session() as session:
            query = select(CrawledLink).where(
                or_(CrawledLink.site_content == "", CrawledLink.site_content.is_(None))
            )
            if limit:
                query = query.limit(limit)
            result = session.execute(query)
            return result.scalars().all()

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
                    "site_content": insert_stmt.excluded.site_content,
                    "updated_at": datetime.now(timezone.utc),
                },
            )

            try:
                self.connection.execute(upsert_stmt)
                self.connection.commit()
                self.logger.info(f"Flushed batch of {len(self._batch)} records")
            except Exception as e:
                self.logger.error(f"Error in batch flush: {e}")
                await self.rollback()
                raise
            finally:
                self._batch.clear()
        except Exception as e:
            self.logger.error(f"Error flushing batch: {e}")
            await self.rollback()
            raise

    def close_database_connection(self):
        try:
            if self._batch:
                self.flush_batch()
            if self.connection:
                self.connection.rollback()  # Ensure any pending transaction is rolled back
                self.connection.close()
            if self.session_instance:
                self.session_instance.rollback()  # Ensure any pending transaction is rolled back
                self.session_instance.close()
            self.logger.info("Database connections closed")
        except Exception as e:
            self.logger.error(f"Error closing database connections: {e}")

    def sample_records_by_group(
        self,
        model: Type[Any],
        group_attr: str,
        sample_size: int = 3,
        filters: List[Any] = None,
    ) -> Dict[Any, List[Any]]:
        """
        Sample records from the database, grouped by a specified attribute.

        :param model: The ORM model class representing the table.
        :param group_attr: The attribute name to group by (as a string).
        :param sample_size: Number of records to sample from each group.
        :param filters: Optional list of filters to apply to the query.
        :return: Dictionary with group keys and lists of sampled records.
        """
        with self.Session() as session:
            # Build the base query
            query = session.query(model)
            if filters:
                query = query.filter(*filters)

            # Fetch all records
            all_records = query.all()

            # Group records by the specified attribute
            grouped_records = defaultdict(list)
            for record in all_records:
                group_value = getattr(record, group_attr)
                grouped_records[group_value].append(record)

            # Sample records from each group
            sampled_records = {}
            for group_value, records in grouped_records.items():
                sampled_records[group_value] = random.sample(
                    records, min(sample_size, len(records))
                )

            return sampled_records
