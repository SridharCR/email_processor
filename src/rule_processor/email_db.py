from datetime import datetime

from typing import List

from sqlalchemy import ForeignKey, String, DateTime, BigInteger, Text, Integer
from sqlalchemy.orm import Mapped, DeclarativeBase, MappedAsDataclass
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from lib.db import postgresql_engine
from lib.log import logger


class Base(MappedAsDataclass, DeclarativeBase):
    """subclasses will be converted to dataclasses"""


class EmailMetadata(Base):
    __tablename__ = "email_metadata"

    id: Mapped[int] = mapped_column(String(16), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(16))
    history_id: Mapped[str] = mapped_column(String(16))
    email_from: Mapped[str] = mapped_column(String(100))
    email_to: Mapped[str] = mapped_column(String(100))
    received_date: Mapped[datetime] = mapped_column(DateTime)
    subject: Mapped[str] = mapped_column(Text)
    size_estimate: Mapped[int] = mapped_column(BigInteger)
    email_body: Mapped[List["EmailBody"]] = relationship()

    @classmethod
    def getattr(cls, field):
        if cls.__dict__.get(field) is not None:
            return cls.__dict__.get(field)
        raise Exception("field doesnt exist")


class EmailBody(Base):
    """User class will be converted to a dataclass"""

    __tablename__ = "email_body"

    id: Mapped[int] = mapped_column(ForeignKey("email_metadata.id"), primary_key=True)
    part_id: Mapped[str] = mapped_column(Integer, primary_key=True)
    size: Mapped[datetime] = mapped_column(BigInteger)
    data: Mapped[datetime] = mapped_column(Text)

    @classmethod
    def getattr(cls, field):
        if cls.__dict__.get(field) is not None:
            return cls.__dict__.get(field)
        raise Exception("field doesnt exist")


Base.metadata.create_all(postgresql_engine)


def email_db_cleanup():
    logger.info("Email database cleanup initiated")
    Base.metadata.drop_all(postgresql_engine)
    logger.info("Email database cleanup completed")
    logger.info("Email database setup initiated")
    Base.metadata.create_all(postgresql_engine)
    logger.info("Email database setup completed")
