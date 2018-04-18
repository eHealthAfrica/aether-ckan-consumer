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
engine = None
Session = None


def init(url):
    global engine
    engine = create_engine(url, connect_args={'check_same_thread': False})

    try:
        Base.metadata.create_all(engine)

        global Session
        Session = sessionmaker(bind=engine)

        logger.info('Database initialized.')
    except SQLAlchemyError:
        logger.error('Database could not be initialized.')
        sys.exit(1)


def get_session():
    return Session()


def get_engine():
    return engine


def make_uuid():
    return str(uuid.uuid4())


class Resource(Base):
    __tablename__ = 'resource'

    resource_id = Column(String, primary_key=True, default=make_uuid)
    resource_name = Column(String, nullable=False)
    ckan_server_id = Column(String, ForeignKey('ckan_server.ckan_server_id'))
    dataset_name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    @classmethod
    def create(cls, **kwargs):
        resource = cls(**kwargs)
        session = get_session()

        session.add(resource)
        session.commit()

        return resource

    @classmethod
    def get_by_name(cls, resource_name):
        session = get_session()
        resource = session.query(cls).filter_by(
            resource_name=resource_name
        ).first()

        return resource


class CkanServer(Base):
    __tablename__ = 'ckan_server'

    ckan_server_id = Column(String, primary_key=True, default=make_uuid)
    ckan_server_url = Column(String, nullable=False)

    @classmethod
    def create(cls, ckan_server_url):
        ckan_server = cls(ckan_server_url=ckan_server_url)

        session = get_session()
        session.add(ckan_server)
        session.commit()

        return ckan_server

    @classmethod
    def get_by_url(cls, ckan_server_url):
        session = get_session()
        ckan_server = session.query(cls).filter_by(
            ckan_server_url=ckan_server_url
        ).first()

        return ckan_server
