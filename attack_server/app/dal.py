from connection.mysql import MySqlConn

class MySqlDal:

    def __init__(self, mysql_client: MySqlConn):
        self._client = mysql_client
        

    def insert_into_attacks(self, attack_id, timestamp, entity_id, weapon_type):
        query = """
        INSERT INTO attacks (attack_id, timestamp, entity_id, weapon_type) 
        VALUES (%s, %s, %s, %s)
        """

        with self._client.cursor() as cursor:

            cursor.execute(query, (attack_id, 
                                   timestamp, 
                                   entity_id, 
                                   weapon_type, 
                                  ))
            
            result = cursor.rowcount

        return result


