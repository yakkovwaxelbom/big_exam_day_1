from pydantic_settings import BaseSettings

class KafkaConsumerConfig(BaseSettings):
    BOOTSTRAP_SERVERS: str = '127.0.0.1:9092'
    GROUP_ID: str = 'attack_worker_v:2'
    ENABLE_AUTO_COMMIT: bool = False
    AUTO_OFFSET_RESET: str = 'earliest'
    TOPIC_ERROR: str = 'intel_signals_dlq'

    def to_dict(self):
        return {
            k.replace('_', '.').lower(): v
            for k, v in self.model_dump().items()}
