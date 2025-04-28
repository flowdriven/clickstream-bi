"""Module for testing utilities."""

import unittest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from src.aws_utils import get_file_from_s3, put_file_into_s3, write_event

class TestAWSUtils(unittest.TestCase):
    """Class testing utilities for managing aws resources"""
    @patch('src.aws_utils.session.client')
    def test_get_file_from_s3(self, mock_session_client):
        """Function testing file retrieve from s3 bucket"""
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_session_client.return_value = mock_s3

        # Mock S3 response
        mock_data = MagicMock()
        mock_s3.get_object.return_value = mock_data

        # Call the function
        result = get_file_from_s3('test_key')

        # Assertions
        mock_session_client.assert_called_once_with('s3', endpoint_url='http://minio:9000')
        mock_s3.get_object.assert_called_once_with(Bucket='data', Key='test_key')
        self.assertEqual(result, mock_data)

    @patch('src.aws_utils.session.client')
    def test_get_file_from_s3_no_such_key(self, mock_session_client):
        """Function testing error file retrieve from s3 bucket"""
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_session_client.return_value = mock_s3

        # Mock S3 response with ClientError
        mock_s3.get_object.side_effect = ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')

        # Call the function
        result = get_file_from_s3('test_key')

        # Assertions
        mock_session_client.assert_called_once_with('s3', endpoint_url='http://minio:9000')
        mock_s3.get_object.assert_called_once_with(Bucket='data', Key='test_key')
        self.assertIsNone(result)

    @patch('src.aws_utils.session.client')
    def test_put_file_into_s3(self, mock_session_client):
        """Function testing file put into s3 bucket"""
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_session_client.return_value = mock_s3
        # Mock data buffer
        mock_data_buffer = MagicMock()
        mock_data_buffer.getvalue.return_value = 'test_data'

        # Call the function
        put_file_into_s3(mock_data_buffer, 'test_key')

        # Assertions
        mock_session_client.assert_called_once_with('s3', endpoint_url='http://minio:9000')
        mock_s3.put_object.assert_called_once_with(Bucket='data', Key='test_key', Body='test_data')

    @patch('src.aws_utils.put_file_into_s3')
    def test_write_event(self, mock_put_file_into_s3):
        """Function testing new file event"""
        # Mock record and offset
        record = '{"event_time": "2025-04-28T10:00:00Z", "event_type": "test_event"}'
        offset = '123'

        # Call the function
        result = write_event(record, offset)

        # Expected key
        expected_key = '/raw/25-04-28_10-00-00_offset_123_test_event.json'

        # Assertions
        mock_put_file_into_s3.assert_called_once()
        self.assertEqual(result, expected_key)

if __name__ == '__main__':
    unittest.main()
