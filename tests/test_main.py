import pytest
from kafkatest.main import KafkaTest


class TestMain:

    @pytest.fixture
    def kafka_test(self, mocker):
        kafka_test = KafkaTest()
        self.producer_stub = mocker.stub()
        self.producer_stub.send = mocker.stub()
        self.consumer_stub = mocker.stub()
        kafka_test.configure_producer('topic', self.producer_stub)
        kafka_test.configure_consumer(self.consumer_stub)
        return kafka_test

    def test_configure_producer_sets_topic_and_producer(self, kafka_test):
        assert kafka_test.producer_topic is 'topic'
        assert kafka_test.producer is self.producer_stub

    def test_configure_consumer_sets_consumer(self, kafka_test):
        assert kafka_test.consumer is self.consumer_stub

    def test_send_one_sends_and_receives_message(self, kafka_test, mocker):
        consumer_stub = iter(["test_message_transformed"])
        kafka_test.configure_consumer(consumer_stub)
        kafka_test.send_one("test_message")
        self.producer_stub.send.assert_called_once()
        assert kafka_test.messages[0]['input'] is "test_message"
        assert kafka_test.messages[0]['output'] is "test_message_transformed"

    def test_send_one_reports_latency(self, kafka_test, mocker):
        consumer_stub = iter(["test_message_transformed"])
        kafka_test.configure_consumer(consumer_stub)
        kafka_test.send_one("test_message")
        self.producer_stub.send.assert_called_once()
        assert kafka_test.messages[0]['latency'] is not None

    def test_all_messages_gets_all_sent_and_received(self, kafka_test):
        kafka_test.messages = [{"input": "test_message", "output": "test_message_transformed", "latency": "7.30384"},
                               {"input": "test_message2", "output": "test_message_transformed2", "latency": "7.30384"}]
        result = kafka_test.all_messages()
        print(result)
        assert result == "Sent message: test_message\nReceived message: test_message_transformed\n" \
                         "End-to-end latency: 7.30384\n\nSent message: test_message2\nReceived message: " \
                         "test_message_transformed2\nEnd-to-end latency: 7.30384\n\n"






