import pytest
from kafkatest.main import KafkaTest
from collections import namedtuple
from pytest_mock import mocker

Message = namedtuple('Message', 'key value topic partition offset')

def poll(timeout_ms=None):
    test_msg = Message("key",  "test_message_transformed", "topic", 0, 14)
    return {"tp": [test_msg]}

def poll_no_match(timeout_ms=None):
    test_msg = Message("key",  "test_message_transformed_not_matching", "topic", 0, 14)
    return {"tp": [test_msg]}

class TestMain:

    @pytest.fixture
    def kafka_test(self, mocker):
        kafka_test = KafkaTest()

        self.producer_stub = mocker.stub()
        self.producer_stub.send = mocker.stub()
        future = mocker.stub()
        future.get = mocker.stub()
        future.get.return_value = Message("key",  "test_message_transformed", "topic", 0, 14)
        self.producer_stub.send.return_value = future

        self.consumer_stub = mocker.stub()
        self.consumer_stub.poll = poll

        self.test_msg = Message("key",  "test_message_transformed", "topic", 0, 14)

        kafka_test.configure_producer('topic', self.producer_stub)
        kafka_test.configure_consumer(self.consumer_stub)

        return kafka_test

    def test_configure_producer_sets_topic_and_producer(self, kafka_test):
        assert kafka_test.producer_topic is 'topic'
        assert kafka_test.producer is self.producer_stub

    def test_configure_consumer_sets_consumer(self, kafka_test):
        assert kafka_test.consumer is self.consumer_stub

    def test_send_one_sends_and_receives_message(self, kafka_test, mocker):
        kafka_test.configure_consumer(self.consumer_stub)
        kafka_test.send_one("key", "test_message")
        self.producer_stub.send.assert_called_once()
        assert kafka_test.messages[0]['input'] is "test_message"
        assert kafka_test.messages[0]['output'] is "test_message_transformed"

    def test_send_one_sends_and_receive_times_out_exception(self, kafka_test, mocker):
        self.consumer_stub.poll = mocker.stub()
        self.consumer_stub.poll.return_value = {"tp": []}
        with pytest.raises(Exception, match="Failed to consume message"):
            kafka_test.configure_consumer(self.consumer_stub)
            kafka_test.send_one("key", "test_message")
            self.producer_stub.send.assert_called_once()

    def test_send_one_reports_latency(self, kafka_test, mocker):
        kafka_test.configure_consumer(self.consumer_stub)
        kafka_test.send_one("key", "test_message")
        self.producer_stub.send.assert_called_once()
        assert kafka_test.messages[0]['latency'] is not None

    def test_assert_next_checks_expected_message_match_when_match(self, kafka_test):
        kafka_test.assert_next("key", "test_message",  "test_message_transformed", 100)

    def test_assert_next_checks_expected_message_match_when_match_no_timeout(self, kafka_test):
        kafka_test.assert_next("key", "test_message",  "test_message_transformed")

    def test_assert_next_checks_expected_message_match_when_does_not_match_fails(self, kafka_test):
        self.consumer_stub.poll = poll_no_match
        kafka_test.configure_consumer(self.consumer_stub)

        with pytest.raises(AssertionError, match="Consumed message test_message_transformed_not_" \
                                                 "matching does not match expected_message test_message_transformed"):
            kafka_test.assert_next("key", "test_message",  "test_message_transformed", 100)

    def test_assert_next_latency_fails(self, kafka_test):
        self.consumer_stub.poll = poll
        kafka_test.configure_consumer(self.consumer_stub)

        with pytest.raises(AssertionError, match="exceeds max_latency 0"):
            kafka_test.assert_next("key", "test_message",  "test_message_transformed", 0)

    def test_all_messages_gets_all_sent_and_received(self, kafka_test):
        kafka_test.messages = [{"input": "test_message", "output": "test_message_transformed", "latency": "7.30384"},
                               {"input": "test_message2", "output": "test_message_transformed2", "latency": "7.30384"}]
        result = kafka_test.all_messages()
        assert result == "Sent message: test_message\nReceived message: test_message_transformed\n" \
                         "End-to-end latency: 7.30384\n\nSent message: test_message2\nReceived message: " \
                         "test_message_transformed2\nEnd-to-end latency: 7.30384\n\n"






