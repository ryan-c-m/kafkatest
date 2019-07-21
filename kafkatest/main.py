"""
Main module
"""
import time


class KafkaTest:
    """
    KafkaTest class
    """

    def __init__(self):
        self.producer_topic = None
        self.producer = None
        self.consumer = None
        self.messages = []

    def configure_producer(self, topic, kafka_producer):
        """
        Configure with a KafkaProducer
        :param topic:
        :param kafka_producer:
        :return:
        """
        self.producer_topic = topic
        self.producer = kafka_producer

    def configure_consumer(self, kafka_consumer):
        """
        Configure with a KafkaConsumer
        :param kafka_consumer:
        :return:
        """
        self.consumer = kafka_consumer

    def configure_generator(self):
        """
        Creates a configuration for a dynamically generated stream of kafka messages
        :return:
        """

    def send_one(self, message):
        """
        Send a single, static message via the producer
        :return:
        """
        self.producer.send(self.producer_topic, message)
        start = time.time()
        result = next(self.consumer)
        end = time.time()
        result = {'input': message, 'output': result, 'latency': end - start}
        self.messages.append(result)

    def all_messages(self):
        """
        Returns a formatted string of all messages sent/received
        :return:
        """
        msg_string = ""
        for res in self.messages:
            line = "Sent message: {}\nReceived message: {}\nEnd-to-end latency: {}\n\n"\
                .format(res['input'], res['output'], res['latency'])
            msg_string += line
        return msg_string

    def generate_messages(self, count=None):
        """
        Generate some messages to the producer
        :param count: Number of messages to generate, infinite if not specified.
        :return:
        """

    def stop(self):
        """
        Stop sending messages
        :return:
        """

    def assert_next(self, expected_message, max_latency=None):
        """
        Assert that the next message is as expected
        :param expected:
        :return:
        """

    def assert_all(self, expected_message, max_latency=None):
        """
        Assert all messages
        :param expected:
        :return:
        """
