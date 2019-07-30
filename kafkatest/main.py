"""
Main module
"""
import time
from kafka import KafkaConsumer, KafkaProducer


class KafkaTest:
    """
    KafkaTest class
    """

    def __init__(self, timeout=10):
        self.producer_topic = None
        self.producer = None
        self.consumer = None
        self.messages = {}
        self.timeout = timeout

    def configure_producer(self, topic, kafka_producer):
        """
        Configure with a KafkaProducer
        :param topic: topic to send messages to
        :param kafka_producer: a KafkaProducer
        """
        self.producer_topic = topic
        self.producer = kafka_producer

    def configure_consumer(self, kafka_consumer):
        """
        Configure with a KafkaConsumer
        :param kafka_consumer: a KafkaConsumer
        """
        self.consumer = kafka_consumer

    def send_and_assert(self, key, message, expected_message, max_latency=None):
        """
        Sends a message via the producer, and asserts the expected result from the consumer
        :param key: key for the kafka message
        :param message: message content to send
        :param expected_message: expected message to receive from consumer
        :param max_latency: fail the test if the message isn't consumed by this time
        """
        self.__send_message(key, message)
        self.__get_result(key)

        latency = self.messages[key]['latency']
        if max_latency is not None and latency > max_latency:
            assert False, "Latency of {} exceeds max_latency {}".format(latency, max_latency)

        consumed_msg = self.messages[key]["output"]

        assert consumed_msg == expected_message, "Consumed message {} does not match expected_message {}" \
                .format(consumed_msg, expected_message)

    def __send_message(self, key, message):
        self.producer.send(self.producer_topic, key=key, value=message)
        self.messages[key] = {'input': message}

    def __get_result(self, key):
        start = time.time()
        timeout_time = start + self.timeout
        while time.time() < timeout_time:
            msg_pack = self.consumer.poll()
            for tp, messages in msg_pack.items():
                for result in messages:
                    if result.key == key:
                        end = time.time()
                        self.messages[key]['output'] = result.value
                        self.messages[key]['latency'] = end - start
                        return
        raise Exception("Failed to consume message")

    def all_messages(self):
        """
        Creates a formatted string of all messages sent/received
        :return: formatted string
        """
        msg_string = ""
        for res in self.messages:
            line = "Sent message: {}\nReceived message: {}\nEnd-to-end latency: {}\n\n"\
                .format(res['input'], res['output'], res['latency'])
            msg_string += line
        return msg_string


def main():
    kafkatest = KafkaTest()

    kafkatest.configure_producer("test", KafkaProducer(bootstrap_servers=['kafka:9092']))
    consumer = KafkaConsumer('test',
                             auto_offset_reset='earliest',
                             bootstrap_servers=['kafka:9092'])
    kafkatest.configure_consumer(consumer)
    kafkatest.send_and_assert(b'1', b'msg', b'msg', max_latency=30)

if __name__ == "__main__":
    main()