from connection.mysql import MySqlConn

class MySqlDal:

    def __init__(self, mysql_client: MySqlConn):
        self._client = mysql_client
        

    def insert_into_damage_reports(self, attack_id, timestamp, result):
        query = """
        INSERT INTO damage_reports (attack_id, timestamp, result) 
        VALUES (%s, %s, %s)
        """

        with self._client.cursor() as cursor:

            cursor.execute(query, (attack_id, 
                                   timestamp, 
                                   result, 
                                  ))
            
            result = cursor.rowcount

        return result


