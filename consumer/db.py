from datetime import datetime
import logging
import sys
import os
import uuid

from sqlalchemy import Column, ForeignKey, String, DateTime, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
logger = logging.getLogger(__name__)
session = None
engine = None


def init(url):
    global engine
    engine = create_engine(url)

    try:
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        global session
        session = Session()
        logger.info('Database initialized.')
    except SQLAlchemyError:
        logger.error('Database could not be initialized.')
        sys.exit(1)


def get_session():
    return session


def get_engine():
    return engine


def make_uuid():
    return str(uuid.uuid4())


class Resource(Base):
    __tablename__ = 'resource'

    resource_id = Column(String, primary_key=True, default=make_uuid())
    resource_name = Column(String, nullable=False, unique=True)
    ckan_server_id = Column(String, ForeignKey('ckan_server.ckan_server_id'))
    dataset_name = Column(String, nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class CkanServer(Base):
    __tablename__ = 'ckan_server'

    ckan_server_id = Column(String, primary_key=True, default=make_uuid())
    ckan_server_url = Column(String, nullable=False, unique=True)

    @classmethod
    def create(cls, ckan_server_url):
        ckan_server = cls(ckan_server_url=ckan_server_url)

        session.add(ckan_server)
        session.commit()

        return ckan_server

    @classmethod
    def get_by_url(cls, ckan_server_url):
        ckan_server = session.query(cls).filter_by(
            ckan_server_url=ckan_server_url
        ).first()

        return ckan_server
