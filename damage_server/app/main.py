from connection.connection_manager import ConnectionsManager, KafkaConsumerConnection
from config.logger import log_event
from dal import MySqlDal
from models import DamageReport
from errors import MsgError

TOPIC = ['damage']

class IntelConsumer:
    def __init__(self, dal: MySqlDal):
        self._dal = dal
        
    def handle_consumer(self, data):
        try:
            data = DamageReport(**data)

            self._dal.insert_into_damage_reports(**data.model_dump())

        except Exception as e:
            raise MsgError(str(e))


def main():

    kafka_consumer: KafkaConsumerConnection = ConnectionsManager.get_connection('kafka_consumer')
    my_sql = ConnectionsManager.get_connection('mysql')

    dal = MySqlDal(my_sql)
    consumer = IntelConsumer(dal)

    kafka_consumer.subscribe(TOPIC)
    kafka_consumer.register_func(consumer.handle_consumer)

    kafka_consumer.start_event_loop()

 
if __name__ == '__main__':
    main()