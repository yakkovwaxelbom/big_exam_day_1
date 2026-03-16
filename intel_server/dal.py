from connection.mysql import MySqlConn

class MySqlDal:

    def __init__(self, mysql_client: MySqlConn):
        self._client = mysql_client
        

    def intel_entity_exists(self, entity_id):
    
        query = """
        SELECT 1
        FROM intel_signals
        WHERE entity_id = %s
        LIMIT 1
        """

        result = None

        with self._client.cursor() as cursor:

            cursor.execute(query, (entity_id,))
            result = cursor.fetchone()

        return result


    def intel_entity_lot_lan_timestep_exists(self, entity_id, lat, lon, timestep):
        query = """
            SELECT 1
            FROM intel_signals
            WHERE entity_id = %s AND reported_lat = %s AND reported_lon = %s AND timestep = %s
            LIMIT 1
        """
        result = None

        with self._client.cursor() as cursor:
            cursor.execute(query, (entity_id, lat, lon, timestep))
            result = cursor.fetchone()

        return result


    def get_intel_entity_id_timestamp_cord_by_timestamp_bigger(
            self, entity_id, timestamp):
        
        query = """
        SELECT entity_id, timestamp, reported_lat, reported_lon
        FROM intel_signals
        WHERE entity_id = %s AND timestamp >= %s
        ORDER BY timestamp
        """

        results = []

        with self._client.cursor() as cursor:

            cursor.execute(query, (entity_id, timestamp))
            results = cursor.fetchall()

        return results
    

    def get_one_timestamp_of_entity_before_time_given_timestamp(self, entity_id, timestamp):
        query = """
        SELECT MAX(timestamp)
        FROM intel_signals
        WHERE entity_id = %s AND timestamp < %s
        """

        result = None

        with self._client.cursor() as cursor:
            cursor.execute(query, (entity_id, timestamp))
            result = cursor.fetchone()

        if result and result[0]:
            return result[0]
        
        return timestamp
    
    
    def set_distance_speed_entity_by_entity_id_timestep(self, entity_id, timestamp,
                                                        distance, speed):

        query = """
        UPDATE intel_signals
        SET distance = %s, speed = %s
        WHERE entity_id = %s AND timestamp = %s
        """

        results = 0

        with self._client.cursor() as cursor:

            cursor.execute(query, (distance, speed, entity_id, timestamp))
            results = cursor.rowcount

        return results
    

    def insert_into_intel_signals(self, signal_id, timestamp, entity_id, reported_lat, 
                                  reported_lon, signal_type, priority_level,
                                  speed, distance):
        query = """
        INSERT INTO intel_signals (signal_id, timestamp, entity_id, reported_lat, reported_lon, signal_type, priority_level, speed, distance) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self._client.cursor() as cursor:

            cursor.execute(query, (signal_id, 
                                   timestamp, 
                                   entity_id, 
                                   reported_lat, 
                                   reported_lon, 
                                   signal_type, 
                                   priority_level, 
                                   speed, 
                                   distance))
            
            result = cursor.rowcount

        return result


