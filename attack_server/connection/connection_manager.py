from config.kafka_consumer import KafkaConsumerConfig
from config.mysql_config import MySqlConfig

from connection.kafka_consumer import KafkaConsumerConnection
from connection.mysql import MySqlConn

from config.logger import log_event

logger = log_event

class ConnectionsManager:

    _registry = {
        'mysql': (MySqlConn, MySqlConfig),
        'kafka_consumer': (KafkaConsumerConnection, KafkaConsumerConfig)
    }

    _connections = {}

    @classmethod
    def _load_connection(cls, name: str):
        if name not in cls._registry:
            msg = f'Connection: {name} is not registered'
            log_event('error', msg)

            raise ValueError(msg)

        conn_class, conn_config = cls._registry[name]

        try:
            config = conn_config().to_dict()
            conn = conn_class(**config)

        except Exception as e:
            log_event('error', str(e))
            raise

        log_event('info', f'Success to connect to {name}')

        return conn
    

    @classmethod
    def get_connection(cls, name: str):
        conn = cls._connections.get(name)

        if conn is None:
            conn = cls._connections[name] = cls._load_connection(name)

        return conn