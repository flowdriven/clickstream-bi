"""Module for testing consumer."""

import unittest
from unittest.mock import patch, MagicMock
from src.consumer import process_msg, process_topic

class TestConsumer(unittest.TestCase):
    """Class testing Kafka consumer client""" 
    @patch('src.consumer.aws_utils.write_event')
    def test_process_msg(self, mock_write_event):
        """Function mocking a new event """  
        # Mock message
        mock_msg = MagicMock()
        mock_msg.offset.return_value = 123
        mock_msg.value.return_value = b'{"key": "value"}'

        # Mock write_event return value
        mock_write_event.return_value = 'event_file.txt'

        # Call the function
        result = process_msg(mock_msg)

        # Assertions
        mock_write_event.assert_called_once_with('{"key": "value"}', '123')
        self.assertEqual(result, 'event_file.txt')

    @patch('src.consumer.utils.get_consumer_client')
    @patch('src.consumer.process_msg')
    def test_process_topic(self, mock_process_msg, mock_get_consumer_client):
        """Function mocking a new topic """  
        # Mock consumer client
        mock_consumer = MagicMock()
        mock_get_consumer_client.return_value = mock_consumer

        # Mock message
        mock_msg = MagicMock()
        mock_msg.error.return_value = None

        # Mock poll method to return a message and then None
        mock_consumer.poll.side_effect = [mock_msg, None, None]

        # Mock process_msg return value
        mock_process_msg.return_value = 'event_file.txt'

        # Call the function
        process_topic('test_topic', 'test_process')

        # Assertions
        mock_consumer.subscribe.assert_called_once_with(
            ['test_topic'],
            on_assign=mock_consumer.subscribe.call_args[1]['on_assign']
        )
        mock_consumer.store_offsets.assert_called_once_with(mock_msg)
        mock_process_msg.assert_called_once_with(mock_msg)

if __name__ == '__main__':
    unittest.main()
