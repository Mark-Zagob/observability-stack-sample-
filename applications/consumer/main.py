# applications/consumer/main.py

import json
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_batch
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Info,
    start_http_server,
)
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode, SpanKind

from config import settings
from logger import setup_logger
from tracing import tracer

# ===========================================
# Logging Setup
# ===========================================
logger = setup_logger()

# ===========================================
# Prometheus Metrics
# ===========================================

APP_INFO = Info(
    'pipeline_consumer_info',
    'Consumer application information'
)
APP_INFO.info({
    'version': '2.0.0',
    'kafka_topic': settings.kafka_topic,
    'consumer_group': settings.kafka_consumer_group,
    'pattern': 'RED'
})

MESSAGES_CONSUMED = Counter(
    'pipeline_consumer_messages_total',
    'Total number of messages consumed',
    ['topic', 'status', 'category']
)

BATCHES_PROCESSED = Counter(
    'pipeline_consumer_batches_total',
    'Total number of batches processed',
    ['status']
)

DB_OPERATIONS = Counter(
    'pipeline_consumer_db_operations_total',
    'Total database operations',
    ['operation', 'status']
)

ERRORS = Counter(
    'pipeline_consumer_errors_total',
    'Total number of errors by type and stage',
    ['error_type', 'stage']
)

MESSAGE_PROCESS_DURATION = Histogram(
    'pipeline_consumer_message_duration_seconds',
    'Time to process a single message',
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
)

BATCH_PROCESS_DURATION = Histogram(
    'pipeline_consumer_batch_duration_seconds',
    'Time to process a batch of messages',
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
)

DB_QUERY_DURATION = Histogram(
    'pipeline_consumer_db_query_duration_seconds',
    'Database query duration',
    ['operation'],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5)
)

KAFKA_POLL_DURATION = Histogram(
    'pipeline_consumer_kafka_poll_duration_seconds',
    'Kafka poll duration',
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0)
)

END_TO_END_LATENCY = Histogram(
    'pipeline_consumer_end_to_end_latency_seconds',
    'End-to-end latency from order creation to database insert',
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)
)

ORDERS_PROCESSED = Counter(
    'pipeline_business_orders_processed_total',
    'Total orders processed by category',
    ['category']
)

REVENUE_PROCESSED = Counter(
    'pipeline_business_revenue_processed_total',
    'Total revenue processed',
    ['category']
)

CONSUMER_UP = Gauge(
    'pipeline_consumer_up',
    'Consumer is running (1) or not (0)'
)

KAFKA_CONNECTION_STATUS = Gauge(
    'pipeline_consumer_kafka_connected',
    'Kafka connection status'
)

DB_CONNECTION_STATUS = Gauge(
    'pipeline_consumer_db_connected',
    'Database connection status'
)

CONSUMER_LAG = Gauge(
    'pipeline_consumer_lag_messages',
    'Consumer lag per partition',
    ['topic', 'partition']
)

CURRENT_BATCH_SIZE = Gauge(
    'pipeline_consumer_current_batch_size',
    'Number of messages in current batch'
)

LAST_PROCESS_TIMESTAMP = Gauge(
    'pipeline_consumer_last_process_timestamp',
    'Timestamp of last successful process'
)

LAST_COMMIT_TIMESTAMP = Gauge(
    'pipeline_consumer_last_commit_timestamp',
    'Timestamp of last offset commit'
)


# ===========================================
# Database Handler
# ===========================================

class DatabaseHandler:
    def __init__(self):
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Connect to PostgreSQL with retry logic"""
        max_retries = 30
        retry_interval = 2
        
        for attempt in range(max_retries):
            try:
                self.conn = psycopg2.connect(settings.postgres_dsn)
                self.conn.autocommit = False
                logger.info(
                    "Connected to PostgreSQL",
                    extra={
                        "event": "db_connected",
                        "host": settings.postgres_host,
                        "database": settings.postgres_db,
                        "attempt": attempt + 1
                    }
                )
                DB_CONNECTION_STATUS.set(1)
                return
            except psycopg2.Error as e:
                DB_CONNECTION_STATUS.set(0)
                ERRORS.labels(error_type="connection", stage="db_init").inc()
                logger.warning(
                    "PostgreSQL connection failed, retrying...",
                    extra={
                        "event": "db_connection_retry",
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                        "error": str(e)
                    }
                )
                time.sleep(retry_interval)
        
        raise Exception("Failed to connect to PostgreSQL after maximum retries")
    
    def insert_orders(self, orders: list[dict], parent_span=None) -> int:
        """Insert batch of orders into database with tracing"""
        if not orders:
            return 0
        
        with tracer.start_as_current_span(
            "db_insert_orders",
            kind=SpanKind.CLIENT
        ) as span:
            span.set_attribute("db.system", "postgresql")
            span.set_attribute("db.name", settings.postgres_db)
            span.set_attribute("db.operation", "INSERT")
            span.set_attribute("db.batch_size", len(orders))
            
            insert_sql = """
                INSERT INTO ecommerce.orders (
                    order_id, customer_id, product_id, product_name, 
                    category, quantity, unit_price, total_amount,
                    order_status, created_at, processed_at
                ) VALUES (
                    %(order_id)s, %(customer_id)s, %(product_id)s, %(product_name)s,
                    %(category)s, %(quantity)s, %(unit_price)s, %(total_amount)s,
                    %(order_status)s, %(created_at)s, %(processed_at)s
                )
                ON CONFLICT (order_id) DO NOTHING
            """
            
            try:
                start_time = time.time()
                
                with self.conn.cursor() as cur:
                    process_time = datetime.now(timezone.utc)
                    
                    for order in orders:
                        order['processed_at'] = process_time.isoformat()
                        if isinstance(order.get('created_at'), str):
                            order['created_at'] = datetime.fromisoformat(
                                order['created_at'].replace('Z', '+00:00')
                            )
                    
                    execute_batch(cur, insert_sql, orders, page_size=100)
                    inserted = cur.rowcount
                    self.conn.commit()
                
                duration = time.time() - start_time
                
                span.set_attribute("db.rows_affected", inserted)
                span.set_attribute("duration_ms", round(duration * 1000, 2))
                span.set_status(Status(StatusCode.OK))
                
                DB_QUERY_DURATION.labels(operation="insert_batch").observe(duration)
                DB_OPERATIONS.labels(operation="insert", status="success").inc(len(orders))
                
                logger.debug(
                    "Orders inserted into database",
                    extra={
                        "event": "db_insert_success",
                        "count": inserted,
                        "duration_ms": round(duration * 1000, 2)
                    }
                )
                
                return inserted
                
            except psycopg2.Error as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                
                ERRORS.labels(error_type=type(e).__name__, stage="db_insert").inc()
                DB_OPERATIONS.labels(operation="insert", status="failed").inc(len(orders))
                
                logger.error(
                    "Database insert failed",
                    extra={
                        "event": "db_insert_error",
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "batch_size": len(orders)
                    }
                )
                self.conn.rollback()
                
                try:
                    self._connect()
                except Exception:
                    DB_CONNECTION_STATUS.set(0)
                
                return 0
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            DB_CONNECTION_STATUS.set(0)
            logger.info("Database connection closed", extra={"event": "db_closed"})


# ===========================================
# Kafka Consumer
# ===========================================

class OrderConsumer:
    def __init__(self, db_handler: DatabaseHandler):
        self.consumer = None
        self.db = db_handler
        self._connect()
    
    def _connect(self):
        """Connect to Kafka with retry logic"""
        max_retries = 30
        retry_interval = 2
        
        for attempt in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    settings.kafka_topic,
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    group_id=settings.kafka_consumer_group,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    max_poll_records=settings.batch_size,
                )
                logger.info(
                    "Connected to Kafka",
                    extra={
                        "event": "kafka_connected",
                        "bootstrap_servers": settings.kafka_bootstrap_servers,
                        "topic": settings.kafka_topic,
                        "consumer_group": settings.kafka_consumer_group,
                        "attempt": attempt + 1
                    }
                )
                CONSUMER_UP.set(1)
                KAFKA_CONNECTION_STATUS.set(1)
                return
            except KafkaError as e:
                KAFKA_CONNECTION_STATUS.set(0)
                ERRORS.labels(error_type="connection", stage="kafka_init").inc()
                logger.warning(
                    "Kafka connection failed, retrying...",
                    extra={
                        "event": "kafka_connection_retry",
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                        "error": str(e)
                    }
                )
                time.sleep(retry_interval)
        
        raise Exception("Failed to connect to Kafka after maximum retries")
    
    def process_messages(self):
        """Main processing loop with tracing"""
        batch: list[dict] = []
        
        logger.info("Starting message processing loop", extra={"event": "processing_started"})
        
        try:
            while True:
                # Poll for messages
                with tracer.start_as_current_span(
                    "kafka_poll",
                    kind=SpanKind.CONSUMER
                ) as poll_span:
                    poll_start = time.time()
                    records = self.consumer.poll(
                        timeout_ms=settings.poll_timeout_ms,
                        max_records=settings.batch_size
                    )
                    poll_duration = time.time() - poll_start
                    
                    poll_span.set_attribute("messaging.system", "kafka")
                    poll_span.set_attribute("messaging.operation", "poll")
                    poll_span.set_attribute("duration_ms", round(poll_duration * 1000, 2))
                    
                    KAFKA_POLL_DURATION.observe(poll_duration)
                
                if not records:
                    if batch:
                        self._process_batch(batch)
                        batch = []
                    continue
                
                # Process received messages
                for topic_partition, messages in records.items():
                    for message in messages:
                        try:
                            msg_start = time.time()
                            order = message.value
                            category = order.get("category", "unknown")
                            
                            # Create span for consuming
                            with tracer.start_as_current_span(
                                "consume_message",
                                kind=SpanKind.CONSUMER
                            ) as msg_span:
                                # Add span attributes
                                msg_span.set_attribute("messaging.system", "kafka")
                                msg_span.set_attribute("messaging.destination", settings.kafka_topic)
                                msg_span.set_attribute("messaging.kafka.partition", topic_partition.partition)
                                msg_span.set_attribute("messaging.kafka.offset", message.offset)
                                msg_span.set_attribute("order.id", order.get("order_id", ""))
                                msg_span.set_attribute("order.category", category)
                                
                                # Link to producer trace if available
                                if "trace_id" in order:
                                    msg_span.set_attribute("producer.trace_id", order["trace_id"])
                                
                                batch.append(order)
                                
                                MESSAGES_CONSUMED.labels(
                                    topic=settings.kafka_topic,
                                    status="success",
                                    category=category
                                ).inc()
                                
                                msg_duration = time.time() - msg_start
                                MESSAGE_PROCESS_DURATION.observe(msg_duration)
                                
                                # Calculate end-to-end latency
                                if 'created_at' in order:
                                    try:
                                        created_at = datetime.fromisoformat(
                                            order['created_at'].replace('Z', '+00:00')
                                        )
                                        e2e_latency = (datetime.now(timezone.utc) - created_at).total_seconds()
                                        END_TO_END_LATENCY.observe(e2e_latency)
                                        msg_span.set_attribute("e2e_latency_seconds", e2e_latency)
                                    except Exception:
                                        pass
                                
                                CONSUMER_LAG.labels(
                                    topic=topic_partition.topic,
                                    partition=str(topic_partition.partition)
                                ).set(message.offset)
                                
                                msg_span.set_status(Status(StatusCode.OK))
                                
                                logger.info(
                                    "Message consumed",
                                    extra={
                                        "event": "message_consumed",
                                        "order_id": order.get("order_id"),
                                        "trace_id": order.get("trace_id", ""),
                                        "category": category,
                                        "partition": topic_partition.partition,
                                        "offset": message.offset
                                    }
                                )
                                
                        except Exception as e:
                            MESSAGES_CONSUMED.labels(
                                topic=settings.kafka_topic,
                                status="failed",
                                category="unknown"
                            ).inc()
                            
                            ERRORS.labels(
                                error_type=type(e).__name__,
                                stage="message_parse"
                            ).inc()
                            
                            logger.error(
                                "Failed to process message",
                                extra={
                                    "event": "message_parse_error",
                                    "error": str(e),
                                    "error_type": type(e).__name__,
                                    "partition": topic_partition.partition,
                                    "offset": message.offset
                                }
                            )

                
                CURRENT_BATCH_SIZE.set(len(batch))
                
                if len(batch) >= settings.batch_size:
                    self._process_batch(batch)
                    batch = []
                    
        except KeyboardInterrupt:
            logger.info("Shutdown requested", extra={"event": "shutdown_requested"})
            if batch:
                self._process_batch(batch)
    
    def _process_batch(self, batch: list[dict]):
        """Process and commit a batch of messages with tracing"""
        if not batch:
            return
        
        with tracer.start_as_current_span(
            "process_batch",
            kind=SpanKind.INTERNAL
        ) as batch_span:
            batch_span.set_attribute("batch.size", len(batch))
            
            start_time = time.time()
            
            # Insert to database
            inserted = self.db.insert_orders(batch)
            
            # Commit Kafka offsets
            self.consumer.commit()
            LAST_COMMIT_TIMESTAMP.set(time.time())
            
            duration = time.time() - start_time
            
            batch_span.set_attribute("batch.inserted", inserted)
            batch_span.set_attribute("duration_ms", round(duration * 1000, 2))
            
            BATCH_PROCESS_DURATION.observe(duration)
            
            if inserted == len(batch):
                batch_span.set_status(Status(StatusCode.OK))
                BATCHES_PROCESSED.labels(status="success").inc()
            elif inserted == 0:
                batch_span.set_status(Status(StatusCode.ERROR, "No records inserted"))
                BATCHES_PROCESSED.labels(status="failed").inc()
            else:
                batch_span.set_status(Status(StatusCode.OK))
                BATCHES_PROCESSED.labels(status="partial").inc()
            
            # Business metrics
            for order in batch:
                category = order.get("category", "unknown")
                ORDERS_PROCESSED.labels(category=category).inc()
                REVENUE_PROCESSED.labels(category=category).inc(order.get("total_amount", 0))
            
            LAST_PROCESS_TIMESTAMP.set(time.time())
            CURRENT_BATCH_SIZE.set(0)
            
            logger.info(
                "Batch processed",
                extra={
                    "event": "batch_processed",
                    "batch_size": len(batch),
                    "inserted": inserted,
                    "duration_ms": round(duration * 1000, 2),
                    "throughput_per_sec": round(len(batch) / duration, 2) if duration > 0 else 0
                }
            )
    
    def close(self):
        """Close the consumer"""
        if self.consumer:
            self.consumer.close()
            CONSUMER_UP.set(0)
            KAFKA_CONNECTION_STATUS.set(0)
            logger.info("Consumer closed", extra={"event": "consumer_closed"})


# ===========================================
# Main
# ===========================================

def main():
    logger.info(
        "Starting Data Consumer",
        extra={
            "event": "consumer_starting",
            "config": {
                "kafka_servers": settings.kafka_bootstrap_servers,
                "topic": settings.kafka_topic,
                "consumer_group": settings.kafka_consumer_group,
                "postgres_host": settings.postgres_host,
                "postgres_db": settings.postgres_db,
                "batch_size": settings.batch_size,
                "metrics_port": settings.metrics_port,
                "tracing_enabled": settings.otel_enabled,
                "tracing_endpoint": settings.otel_exporter_otlp_endpoint
            }
        }
    )
    
    # Start Prometheus metrics server
    start_http_server(settings.metrics_port)
    logger.info(
        "Metrics server started",
        extra={"event": "metrics_server_started", "port": settings.metrics_port}
    )
    
    # Create handlers
    db_handler = DatabaseHandler()
    consumer = OrderConsumer(db_handler)
    
    try:
        consumer.process_messages()
    except Exception as e:
        ERRORS.labels(error_type="fatal", stage="main").inc()
        logger.error(
            "Fatal error in consumer",
            extra={"event": "fatal_error", "error": str(e), "error_type": type(e).__name__}
        )
    finally:
        consumer.close()
        db_handler.close()
        logger.info("Consumer stopped", extra={"event": "consumer_stopped"})


if __name__ == "__main__":
    main()
