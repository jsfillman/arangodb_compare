import os
import json
import unittest
from unittest.mock import patch, MagicMock
import sys

# Ensure the module is discoverable by adding the parent directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import arangodb_compare.main as arango_compare

class TestArangoCompare(unittest.TestCase):

    @patch('os.getenv')
    def test_get_env_variable(self, mock_getenv):
        mock_getenv.side_effect = lambda var_name, default=None: {'TEST_VAR': 'value'}.get(var_name, default)
        self.assertEqual(arango_compare.get_env_variable('TEST_VAR', 'default'), 'value')
        self.assertEqual(arango_compare.get_env_variable('UNKNOWN_VAR', 'default'), 'default')

    @patch('arangodb_compare.main.ArangoClient')
    def test_connect_to_arango(self, MockArangoClient):
        mock_client = MagicMock()
        mock_db = MagicMock()
        MockArangoClient.return_value = mock_client
        mock_client.db.return_value = mock_db

        url = 'http://localhost:8529'
        db_name = 'test_db'
        username = 'root'
        password = 'passwd'

        client, db = arango_compare.connect_to_arango(url, db_name, username, password)
        self.assertEqual(client, mock_client)
        self.assertEqual(db, mock_db)

    @patch('os.makedirs')
    @patch('os.getcwd')
    @patch('os.getenv')
    @patch('arangodb_compare.main.datetime')
    def test_setup_logging_directory(self, mock_datetime, mock_getenv, mock_getcwd, mock_makedirs):
        mock_datetime.now.return_value.strftime.return_value = '2024-01-01_000000'
        mock_getcwd.return_value = '/current/working/dir'
        mock_getenv.return_value = None

        log_dir, timestamp = arango_compare.setup_logging_directory()
        self.assertEqual(log_dir, '/current/working/dir/2024-01-01_000000')
        self.assertEqual(timestamp, '2024-01-01_000000')
        mock_makedirs.assert_called_once_with('/current/working/dir/2024-01-01_000000', exist_ok=True)

    @patch('builtins.open', unittest.mock.mock_open())
    def test_write_log(self):
        log_dir = '/log/dir'
        entity_type = 'test_entity'
        content = 'Log content'

        arango_compare.write_log(log_dir, entity_type, content)
        with open(os.path.join(log_dir, f'{entity_type}.md'), 'a') as file:
            file.write(content + '\n')
        open.assert_called_with('/log/dir/test_entity.md', 'a')
        open().write.assert_called_with('Log content\n')

    def test_normalize_json(self):
        data = {'b': 1, 'a': 2}
        normalized = arango_compare.normalize_json(data)
        self.assertEqual(normalized, json.dumps(data, sort_keys=True))

    @patch('arangodb_compare.main.fetch_entities')
    @patch('arangodb_compare.main.write_log')
    def test_compare_entities_existence(self, mock_write_log, mock_fetch_entities):
        mock_fetch_entities.side_effect = [
            {'entity1': {}, 'entity2': {}},
            {'entity2': {}, 'entity3': {}}
        ]
        db1 = MagicMock()
        db2 = MagicMock()
        log_dir = '/log/dir'

        arango_compare.compare_entities_existence(log_dir, db1, db2, 'test_entity')

        mock_write_log.assert_called()
        log_content = mock_write_log.call_args[0][1]
        self.assertIn('## Test_entitys unique to db1:', log_content)
        self.assertIn('- entity1', log_content)
        self.assertIn('## Test_entitys unique to db2:', log_content)
        self.assertIn('- entity3', log_content)

    @patch('arangodb_compare.main.fetch_entities')
    @patch('arangodb_compare.main.write_log')
    def test_compare_entities_detail(self, mock_write_log, mock_fetch_entities):
        mock_fetch_entities.side_effect = [
            {'entity1': {'name': 'entity1', 'field': 'value1'}, 'entity2': {'name': 'entity2', 'field': 'value2'}},
            {'entity1': {'name': 'entity1', 'field': 'value1'}, 'entity2': {'name': 'entity2', 'field': 'value2_diff'}}
        ]
        db1 = MagicMock()
        db2 = MagicMock()
        log_dir = '/log/dir'
        ignore_fields = set()

        arango_compare.compare_entities_detail(log_dir, db1, db2, 'test_entity', ignore_fields)

        mock_write_log.assert_called()
        log_content = mock_write_log.call_args[0][1]
        self.assertIn('### Differences in test_entity \'entity2\':', log_content)
        self.assertIn('No differences in test_entity \'entity1\'.', log_content)

    @patch('arangodb_compare.main.fetch_document_ids')
    @patch('arangodb_compare.main.write_log')
    def test_compare_document_ids(self, mock_write_log, mock_fetch_document_ids):
        mock_fetch_document_ids.side_effect = [
            ['doc1', 'doc2', 'doc3'],
            ['doc2', 'doc3', 'doc4']
        ]
        db1 = MagicMock()
        db2 = MagicMock()
        log_dir = '/log/dir'

        arango_compare.compare_document_ids(log_dir, db1, db2, 'test_collection')

        mock_write_log.assert_called()
        log_content = mock_write_log.call_args[0][1]
        self.assertIn('## Document IDs unique to db1:', log_content)
        self.assertIn('- doc1', log_content)
        self.assertIn('## Document IDs unique to db2:', log_content)
        self.assertIn('- doc4', log_content)

    @patch('arangodb_compare.main.fetch_collection_documents')
    @patch('arangodb_compare.main.write_log')
    def test_compare_random_documents(self, mock_write_log, mock_fetch_collection_documents):
        mock_fetch_collection_documents.side_effect = [
            {'doc1': {'_key': 'doc1', 'field': 'value1'}, 'doc2': {'_key': 'doc2', 'field': 'value2'}},
            {'doc1': {'_key': 'doc1', 'field': 'value1'}, 'doc2': {'_key': 'doc2', 'field': 'value2_diff'}}
        ]
        db1 = MagicMock()
        db2 = MagicMock()
        log_dir = '/log/dir'
        sample_size = 2
        ignore_fields = set()

        arango_compare.compare_random_documents(log_dir, db1, db2, 'test_collection', sample_size, ignore_fields)

        mock_write_log.assert_called()
        log_content = mock_write_log.call_args[0][1]
        self.assertIn('### Differences in document \'doc2\'', log_content)
        self.assertIn('No differences in document \'doc1\'.', log_content)

if __name__ == '__main__':
    unittest.main()

