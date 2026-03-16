from pydantic_settings import BaseSettings

class MySqlConfig(BaseSettings):
    MYSQL_HOTS: str = '127.0.0.1'
    MYSQL_POR: int = 3306
    MYSQL_USR: str = 'root'
    MYSQL_PASSWORD: str = '123456'
    MYSQL_DATABASE: str = 'dev'

    def to_dict(self):
        return {
            k.replace('MYSQL_', '.').lower(): v
            for k, v in self.model_dump().items()}
