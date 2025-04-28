"""Module for testing utilities."""

import os
import json
import unittest
from unittest.mock import patch, MagicMock
from src.utils import get_admin, delete_topics, create_topics
from src.utils import get_producer_client, get_consumer_client
from src.utils import write_event

# -----------------------------------------------------------------------------
# Test Admin Client
# -----------------------------------------------------------------------------

class TestAdmin(unittest.TestCase):
    """Class testing Kafka admin client"""
    @patch('src.utils.AdminClient')
    def test_get_admin(self, mock_admin_client):
        """Function mocking Kafka admin client"""
        mock_admin_client_instance = MagicMock()
        mock_admin_client.return_value = mock_admin_client_instance
        admin_client = get_admin()
        self.assertEqual(admin_client, mock_admin_client_instance)

    @patch('src.utils.get_admin')
    def test_delete_topics(self, mock_get_admin):
        """Function testing delete topics"""
        mock_admin_client = MagicMock()
        mock_get_admin.return_value = mock_admin_client
        mock_future = MagicMock()
        mock_future.result.return_value = None
        mock_admin_client.delete_topics.return_value = {'test_topic': mock_future}
        delete_topics(['test_topic'])
        mock_future.result.assert_called_once()

    @patch('src.utils.get_admin')
    @patch('src.utils.NewTopic')
    def test_create_topics(self, mock_new_topic, mock_get_admin):
        """Function testing create topics"""
        mock_admin_client = MagicMock()
        mock_get_admin.return_value = mock_admin_client
        mock_future = MagicMock()
        mock_future.result.return_value = None
        mock_admin_client.create_topics.return_value = {'test_topic': mock_future}
        create_topics(['test_topic'])
        mock_future.result.assert_called_once()

# -----------------------------------------------------------------------------
# Test Producer client
# -----------------------------------------------------------------------------

class TestProducer(unittest.TestCase):
    """Class testing Kafka producer client"""
    @patch('src.utils.Producer')
    def test_get_producer_client(self, mock_producer):
        """Function mocking producer"""
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        producer_client = get_producer_client()
        self.assertEqual(producer_client, mock_producer_instance)

# -----------------------------------------------------------------------------
# Test Consumer client
# -----------------------------------------------------------------------------

class TestConsumer(unittest.TestCase):
    """Class testing Kafka consumer client"""
    @patch('src.utils.Consumer')
    def test_get_consumer_client(self, mock_consumer):
        """Function mocking consumer"""
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance
        consumer_client = get_consumer_client('test_group', 'test_process')
        self.assertEqual(consumer_client, mock_consumer_instance)

# -----------------------------------------------------------------------------
# Test writer service
# -----------------------------------------------------------------------------

class TestWriteEvent(unittest.TestCase):
    """Class testing writing service"""
    def setUp(self):
        """Action before testing"""
        self.record = json.dumps({
            "event_time": "2025-04-25T13:04:42Z",
            "event_type": "test_event"
        })
        self.offset = "12345"
        self.local_data_directory = "./data"
        self.file_path = None

    def tearDown(self):
        """Action after testing"""
        if self.file_path:
            os.remove(self.file_path)

    def test_write_event(self):
        """Fuction mocking event file"""
        filename = write_event(self.record, self.offset)
        expected_filename = "25-04-25_13-04-42_offset_12345_test_event.json"
        self.assertEqual(filename, expected_filename)

        file_path = os.path.join(self.local_data_directory, filename)
        self.assertTrue(os.path.exists(file_path))
        self.file_path = file_path

        with open(file_path, 'r', encoding="utf-8") as f:
            data = json.load(f)
            self.assertEqual(data[0]['event_time'], "2025-04-25T13:04:42Z")
            self.assertEqual(data[0]['event_type'], "test_event")
