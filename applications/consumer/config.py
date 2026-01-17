from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Consumer configuration from environment variables"""
    
    # Kafka settings
    kafka_bootstrap_servers: str = "kafka:29092"
    kafka_topic: str = "ecommerce.orders"
    kafka_consumer_group: str = "order-processor"
    
    # PostgreSQL settings
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "datawarehouse"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres123"
    
    # Consumer settings
    batch_size: int = 10
    poll_timeout_ms: int = 1000
    
    # Metrics server
    metrics_port: int = 8001
    
    # Tracing
    otel_exporter_otlp_endpoint: str = "http://jaeger:4317"
    otel_service_name: str = "data-consumer"
    otel_enabled: bool = True
    
    # Application
    app_name: str = "data-consumer"
    log_level: str = "INFO"
    
    class Config:
        env_prefix = ""
        case_sensitive = False
    
    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


settings = Settings()
