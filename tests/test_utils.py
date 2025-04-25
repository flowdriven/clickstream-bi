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
    @patch('src.utils.AdminClient')
    def test_get_admin(self, MockAdminClient):
        mock_admin_client = MagicMock()
        MockAdminClient.return_value = mock_admin_client
        admin_client = get_admin()
        self.assertEqual(admin_client, mock_admin_client)

    @patch('src.utils.get_admin')
    def test_delete_topics(self, MockGetAdmin):
        mock_admin_client = MagicMock()
        MockGetAdmin.return_value = mock_admin_client
        mock_future = MagicMock()
        mock_future.result.return_value = None
        mock_admin_client.delete_topics.return_value = {'test_topic': mock_future}
        delete_topics(['test_topic'])
        mock_future.result.assert_called_once()

    @patch('src.utils.get_admin')
    @patch('src.utils.NewTopic')
    def test_create_topics(self, MockNewTopic, MockGetAdmin):
        mock_admin_client = MagicMock()
        MockGetAdmin.return_value = mock_admin_client
        mock_future = MagicMock()
        mock_future.result.return_value = None
        mock_admin_client.create_topics.return_value = {'test_topic': mock_future}
        create_topics(['test_topic'])
        mock_future.result.assert_called_once()

# -----------------------------------------------------------------------------
# Test Producer client  
# -----------------------------------------------------------------------------

class TestProducer(unittest.TestCase):
    @patch('src.utils.Producer')
    def test_get_producer_client(self, MockProducer):
        mock_producer = MagicMock()
        MockProducer.return_value = mock_producer
        producer_client = get_producer_client()
        self.assertEqual(producer_client, mock_producer)

# -----------------------------------------------------------------------------
# Test Consumer client  
# -----------------------------------------------------------------------------

class TestConsumer(unittest.TestCase):
    @patch('src.utils.Consumer')
    def test_get_consumer_client(self, MockConsumer):
        mock_consumer = MagicMock()
        MockConsumer.return_value = mock_consumer
        consumer_client = get_consumer_client('test_group', 'test_process')
        self.assertEqual(consumer_client, mock_consumer)

# -----------------------------------------------------------------------------
# Test writer service   
# -----------------------------------------------------------------------------

class TestWriteEvent(unittest.TestCase):
    def setUp(self):
        self.record = json.dumps({
            "event_time": "2025-04-25T13:04:42Z",
            "event_type": "test_event"
        })
        self.offset = "12345"
        self.local_data_directory = "./data"
        self.file_path = None 

    def tearDown(self):
        if self.file_path: 
            os.remove(self.file_path)

    def test_write_event(self):
        filename = write_event(self.record, self.offset)
        expected_filename = "25-04-25_13-04-42_offset_12345_test_event.json"
        self.assertEqual(filename, expected_filename)

        file_path = os.path.join(self.local_data_directory, filename) 
        self.assertTrue(os.path.exists(file_path))
        self.file_path = file_path

        with open(file_path, 'r') as f:
            data = json.load(f)
            self.assertEqual(data[0]['event_time'], "2025-04-25T13:04:42Z")
            self.assertEqual(data[0]['event_type'], "test_event")
