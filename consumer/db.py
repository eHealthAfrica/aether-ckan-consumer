from datetime import datetime
import logging
import sys
import os

from sqlalchemy import Column, ForeignKey, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from config import get_config

Base = declarative_base()
logger = logging.getLogger(__name__)
session = None


def init():
    url = get_config().get('database').get('url')
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


class Resource(Base):
    __tablename__ = 'resource'

    resource_id = Column(String, primary_key=True)
    resource_name = Column(String, nullable=False, unique=True)
    ckan_server_id = Column(String, ForeignKey('ckan_server.ckan_server_id'))
    dataset_name = Column(String, nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class CkanServer(Base):
    __tablename__ = 'ckan_server'

    ckan_server_id = Column(String, primary_key=True)
    ckan_server_url = Column(String, nullable=False, unique=True)
