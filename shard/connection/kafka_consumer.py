from confluent_kafka import Consumer, Producer, Message, KafkaError
from typing import Dict, Callable, List
import json


from config.logger import log_event

logger = log_event


class MsgError(Exception):
    def __init__(self, message):
        super().__init__(message)

    def __str__(self):
        return f"{self.message}"

class KafkaConsumerConnection:

    _instance = None
    _initialized = False


    def __new__(cls, **config):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, **config):
        if not self._initialized:

            producer_config = {
                'bootstrap.servers': config['bootstrap.servers']
            }

            self._error_topic = config.pop('topic.error')

            self._client = Consumer(config)
            self._func_event_loop: Callable = None

            self._producer_errors = Producer(producer_config)

            self._initialized = True


    def subscribe(self, topics: List[str]):
        self._client.subscribe(topics)


    def register_func(self, handle: str, func: Callable):
        self._func_event_loop = func


    def start_event_loop(self, num_commit_count=10):
        try:
            msg_count = 0

            while True:

                try:

                    msg = self._client.poll(1.0)

                    if not msg: continue

                    if self._handle_errors(msg): continue

                    self._msg_process(msg)

                    msg_count += 1

                    if msg_count == num_commit_count:
                        self._client.commit(asynchronous=False)
                        msg_count = 1

                except MsgError as e:

                    value = {'msg': msg,
                             'error': str(e)}
                    
                    value = json.dumps(value).encode("utf-8")

                    self._producer_errors.produce(topic=self._error_topic,
                                                  value=value,
                                                  callback=self._delivery_error_msg_report)
                    
                    log_event(f'ERROR: {value}')

        except KeyboardInterrupt:
            log_event('INFO: Stopping consumer"')

        finally:
            self._client.close()


    def _handle_errors(msg: Message):
        if e := msg.error():

            if e.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                log_event(f'WARING: unknown topic: {msg.topic} or part: {msg.partition}')
         
            else:
                log_event(f'ERROR: unknown error {str(e)}')

        return e != None
    
    def _msg_process(self, msg: Message):

        data_str = msg.value().decode(encoding='utf-8')

        try:
            data_dict: Dict = json.loads(data_str)

            self._func_event_loop(data_dict)

        except json.decoder.JSONDecodeError as e:
            raise MsgError(str(e))


    def _delivery_error_msg_report(err, msg: Message):
        if err:
            log_event(f"ERROR: {msg.value().decode()} Delivery error failed: {err}")
        else:
            log_event(f"WARING: Error delivered {msg.value().decode("utf-8")}")





         