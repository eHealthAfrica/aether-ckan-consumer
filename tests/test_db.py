import unittest

import sqlalchemy

from consumer import db


class TestDB(unittest.TestCase):
    def test_init(self):
        url = 'sqlite:////srv/app/db/test.db'
        db.init(url)

        engine = db.get_engine()
        created_tables = engine.table_names()
        expected_tables = ['ckan_server', 'resource']

        assert len(created_tables) == len(expected_tables)
        assert set(created_tables).issuperset(set(expected_tables)) is True
