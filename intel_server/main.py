from connection.connection_manager import ConnectionsManager, KafkaConsumerConnection
from config.logger import log_event
from dal import MySqlDal
from models import IntelSignal
from errors import MsgError
from utils.haversine import haversine_km

TOPIC = ['intel']


class IntelConsumer:
    def __init__(self, dal: MySqlDal):
        self._dal = dal
        
    def handle_consumer(self, data):
        try:
            data = IntelSignal(**data)

            if self._dal.intel_entity_lot_lan_timestep_exists(data.entity_id,
                                                              data.timestamp,
                                                              data.reported_lon,
                                                              data.reported_lat):
                
                raise MsgError(f'There is a conflict. A report already' 
                               f'exists at {data.timestamp} but the location is different.')

            if not self._dal.intel_entity_exists(data.entity_id):
                data.entity_id = 0
            
            self._dal.insert_into_intel_signals(**data.model_dump())


            first_timestep_to_update = \
                self._dal.get_one_timestamp_of_entity_before_time_given_timestamp(
                data.entity_id, data.timestamp)
                        
            records_to_update = \
                self._dal.get_intel_entity_id_timestamp_cord_by_timestamp_bigger(
                    data.entity_id, first_timestep_to_update
            )

            if not records_to_update: return

            temp = records_to_update[0]

            for current in records_to_update[1:]:

                distance = haversine_km(temp['reported_lat'], temp['reported_lon'], current['reported_lat'], current['reported_lon'])
                speed = (distance*1000)/(current['timestamp'] - temp['timestamp']).total_seconds()

                self._dal.set_distance_speed_entity_by_entity_id_timestep(current['entity_id'], current['timestamp'], distance, speed)

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

from datetime import datetime

a = (datetime(2026, 3, 16, 7, 49, 43, 430306) - datetime(2026, 3, 16, 7, 49, 43, 430306))