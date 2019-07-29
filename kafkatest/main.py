"""
Main module
"""
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

class KafkaTest:
    """
    KafkaTest class
    """

    def __init__(self):
        self.producer_topic = None
        self.producer = None
        self.consumer = None
        self.messages = []
        self.timeout=10

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

    def send_one(self, key, message):
        """
        Send a single, static message via the producer
        :return:
        """

        future = self.producer.send(self.producer_topic, key=key, value=message)
        record_metadata = future.get(timeout=10)

        start = time.time()
        timeout_time = start + self.timeout
        while time.time() < timeout_time:
            msg_pack = self.consumer.poll()
            for tp, messages in msg_pack.items():
                for result in messages:
                    if result.key == key:
                        end = time.time()
                        result = {'input': message, 'output': result.value, 'latency': end - start,
                                  'metadata': record_metadata}
                        self.messages.append(result)
                        return result

        raise Exception("Failed to consume message")

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

    def assert_next(self, key, message, expected_message, max_latency=None):
        """
        Assert that the next message is as expected
        :param expected:
        :return:
        """
        self.send_one(key, message)

        latency = self.messages[-1]['latency']
        if max_latency is not None and latency > max_latency:
            assert False, "Latency of {} exceeds max_latency {}".format(latency, max_latency)

        consumed_msg = self.messages[-1]["output"]

        if consumed_msg == expected_message:
            assert True
        else:
            assert False, "Consumed message {} does not match expected_message {}"\
                .format(consumed_msg, expected_message)

    def assert_all(self, expected_message, max_latency=None):
        """
        Assert all messages
        :param expected:
        :return:
        """

def main():
    kafkatest = KafkaTest()

    kafkatest.configure_producer("test", KafkaProducer(bootstrap_servers=['kafka:9092']))
    consumer = KafkaConsumer('test',
                             auto_offset_reset='earliest',
                             bootstrap_servers=['kafka:9092'])
    kafkatest.configure_consumer(consumer)
    kafkatest.assert_next(b'1', b'msg', b'msg', max_latency=30)

if __name__ == "__main__":
    main()