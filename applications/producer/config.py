# applications/producer/config.py

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Producer configuration from environment variables"""
    
    # Kafka settings
    kafka_bootstrap_servers: str = "kafka:29092"
    kafka_topic: str = "ecommerce.orders"
    
    # Producer settings
    produce_interval_seconds: float = 1.0  # Produce every N seconds
    batch_size: int = 5  # Messages per batch
    
    # Metrics server
    metrics_port: int = 8000

    # Tracing
    otel_exporter_otlp_endpoint: str = "http://jaeger:4317"
    otel_service_name: str = "data-producer"
    otel_enabled: bool = True

    # Application
    app_name: str = "data-producer"
    log_level: str = "INFO"
    
    class Config:
        env_prefix = ""
        case_sensitive = False


settings = Settings()
