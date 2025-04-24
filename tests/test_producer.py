
import unittest
from unittest.mock import patch, MagicMock, mock_open
import json 
from src.producer import process_topic, delivery_report

class TestProducer(unittest.TestCase):

    @patch('src.utils.get_producer_client')
    @patch('src.producer.glob.glob')
    @patch('src.producer.open', new_callable=mock_open, read_data='col1,col2\nval1,val2\nval3,val4')
    def test_process_topic(self, mock_open, mock_glob, mock_get_producer_client):
        # Mock the Kafka producer client
        mock_producer = MagicMock()
        mock_get_producer_client.return_value = mock_producer

        # Mock the glob to return a specific file
        mock_glob.return_value = ['./data/test_topic.csv']

        # Call the process_topic function
        process_topic('test_topic')

        # Check if the producer's produce method was called with the correct parameters
        expected_record_1 = json.dumps({'col1': 'val1', 'col2': 'val2'}).encode('utf-8')
        expected_record_2 = json.dumps({'col1': 'val3', 'col2': 'val4'}).encode('utf-8')

        calls = [
            unittest.mock.call(
                topic='test_topic',
                key=unittest.mock.ANY,
                value=expected_record_1,
                callback=delivery_report
            ),
            unittest.mock.call(
                topic='test_topic',
                key=unittest.mock.ANY,
                value=expected_record_2,
                callback=delivery_report
            )
        ]

        mock_producer.produce.assert_has_calls(calls, any_order=True)
        mock_producer.flush.assert_called_once()

if __name__ == '__main__':
    unittest.main()
