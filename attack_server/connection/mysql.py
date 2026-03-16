from mysql.connector import pooling, MySQLConnection
from contextlib import contextmanager


class MySqlConn:

    _instance = None
    _initialized = False


    def __new__(cls, **config):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, **config):
        if not self._initialized:

            self._pool = pooling.MySQLConnectionPool(
                pool_name='dev',
                pool_size=3,
                **config
            )

            self._initialized = True


    def get_connection(self):
        return self._pool.get_connection()
    
    @contextmanager
    def cursor(self):
        conn = None
        cursor = None

        try:
            conn: MySQLConnection = self.get_connection()
            cursor = conn.cursor(dictionary=True)

            yield cursor

            conn.commit()

        except Exception as e:
            if conn:
                conn.rollback()

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

