import unittest
import uuid

import sqlalchemy

from consumer import db


def gen_uuid():
    return str(uuid.uuid4())


class TestDB(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDB, self).__init__(*args, **kwargs)
        self.db_url = 'sqlite://'

    @classmethod
    def tearDownClass(cls):
        engine = db.get_engine()
        sql = sqlalchemy.text('DROP TABLE IF EXISTS resource;')
        engine.execute(sql)
        sql = sqlalchemy.text('DROP TABLE IF EXISTS ckan_server;')
        engine.execute(sql)

    def test_init(self):
        db.init(self.db_url)

        engine = db.get_engine()
        created_tables = engine.table_names()
        expected_tables = ['ckan_server', 'resource']

        assert len(created_tables) == len(expected_tables)
        assert set(created_tables).issuperset(set(expected_tables)) is True

    def test_get_session(self):
        db.init(self.db_url)

        session = db.get_session()

        assert isinstance(session, sqlalchemy.orm.session.Session) is True

    def test_get_engine(self):
        db.init(self.db_url)

        engine = db.get_engine()

        assert isinstance(engine, sqlalchemy.engine.Engine) is True

    def test_make_uuid(self):
        uuid = db.make_uuid()

        assert len(uuid) == 36

    def test_create_resource(self):
        db.init(self.db_url)

        data = {
            'resource_name': gen_uuid(),
            'dataset_name': gen_uuid(),
            'ckan_server_id': gen_uuid(),
            'resource_id': gen_uuid()
        }

        resource = db.Resource.create(**data)

        assert resource.resource_name == data.get('resource_name')
        assert resource.dataset_name == data.get('dataset_name')
        assert resource.ckan_server_id == data.get('ckan_server_id')
        assert resource.resource_id == data.get('resource_id')

    def test_get_resource(self):
        db.init(self.db_url)

        data = {
            'resource_name': gen_uuid(),
            'dataset_name': gen_uuid(),
            'ckan_server_id': gen_uuid(),
            'resource_id': gen_uuid()
        }

        db.Resource.create(**data)

        resource = db.Resource.get(
            resource_name=data.get('resource_name'),
            ckan_server_id=data.get('ckan_server_id'),
            dataset_name=data.get('dataset_name')
        )

        assert resource.resource_name == data.get('resource_name')
        assert resource.dataset_name == data.get('dataset_name')
        assert resource.ckan_server_id == data.get('ckan_server_id')
        assert resource.resource_id == data.get('resource_id')

    def test_create_ckan_server(self):
        db.init(self.db_url)

        data = {
            'ckan_server_url': gen_uuid()
        }

        ckan_server = db.CkanServer.create(**data)

        assert ckan_server.ckan_server_url == data.get('ckan_server_url')

    def test_get_ckan_server(self):
        db.init(self.db_url)

        data = {
            'ckan_server_url': gen_uuid()
        }

        db.CkanServer.create(**data)

        ckan_server = db.CkanServer.get_by_url(**data)

        assert ckan_server.ckan_server_url == data.get('ckan_server_url')
