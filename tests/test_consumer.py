"""Module for testing consumer."""

import json
import unittest
#from unittest.mock import patch, MagicMock, mock_open
from unittest.mock import patch, MagicMock
from src.consumer import process_msg, process_topic

class TestConsumer(unittest.TestCase):
    """Class testing Kafka consumer client""" 
    @patch('src.utils.get_consumer_client')
    @patch('src.utils.pd.DataFrame.to_json')
    #@patch('src.consumer.open', new_callable=mock_open)
    #def test_process_msg(self, mock_open, mock_to_json, mock_get_consumer_client):
    def test_process_msg(self, mock_to_json, mock_get_consumer_client):
        """Function mocking Kafka consumer client"""        
        # Mock the Kafka consumer client
        mock_consumer = MagicMock()
        mock_get_consumer_client.return_value = mock_consumer

        # Create a mock message
        mock_msg = MagicMock()
        mock_msg.value.return_value = json.dumps({
            'event_time': '2025-04-23T14:00:00Z',
            'event_type': 'test_event'
        }).encode('utf-8')
        mock_msg.offset.return_value = 123

        # Call the process_msg function
        filename = process_msg(mock_msg)

        # Check if the DataFrame's to_json method was called with the correct parameters
        #mock_to_json.assert_called_once_with(f'./data/{filename}.json', orient='records')
        mock_to_json.assert_called_once_with(f'./data/{filename}', orient='records')

        # Check the filename format
        self.assertTrue(filename.startswith('25-04-23_14-00-00_offset_123_test_event'))

    @patch('src.utils.get_consumer_client')
    @patch('src.consumer.process_msg')
    def test_process_topic(self, mock_process_msg, mock_get_consumer_client):
        """Function mocking Kafka consumer client"""
        mock_consumer = MagicMock()
        mock_get_consumer_client.return_value = mock_consumer

        # Create a mock message
        mock_msg = MagicMock()
        mock_msg.value.return_value = json.dumps({
            'event_time': '2025-04-23T14:00:00Z',
            'event_type': 'test_event'
        }).encode('utf-8')
        mock_msg.error.return_value = None

        # Mock the poll method to return the mock message on the first call and then None
        # Responsible to ensure that the loop processes only one message
        mock_consumer.poll.side_effect = [mock_msg, None]

        # Set a return value for the process_msg mock
        mock_process_msg.return_value = '25-04-23_14-00-00_offset_123_test_event'

        # Call the process_topic function
        process_topic('test_topic', 'test_process')

        # Check if the consumer's subscribe method was called with the correct parameters
        mock_consumer.subscribe.assert_called_once_with(['test_topic'], on_assign=unittest.mock.ANY)

        # Check if the process_msg function was called
        mock_process_msg.assert_called_once_with(mock_msg)

if __name__ == '__main__':
    unittest.main()
