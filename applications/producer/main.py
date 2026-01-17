# applications/producer/main.py

import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Info,
    start_http_server,
)
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

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
    'pipeline_producer_info',
    'Producer application information'
)
APP_INFO.info({
    'version': '2.0.0',
    'kafka_topic': settings.kafka_topic,
    'pattern': 'RED'
})

MESSAGES_PRODUCED = Counter(
    'pipeline_producer_messages_total',
    'Total number of messages produced',
    ['topic', 'status', 'category']
)

BATCHES_PRODUCED = Counter(
    'pipeline_producer_batches_total',
    'Total number of batches produced',
    ['status']
)

ERRORS = Counter(
    'pipeline_producer_errors_total',
    'Total number of errors by type',
    ['error_type', 'stage']
)

MESSAGE_PRODUCE_DURATION = Histogram(
    'pipeline_producer_message_duration_seconds',
    'Time to produce a single message to Kafka',
    ['topic'],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0)
)

BATCH_PRODUCE_DURATION = Histogram(
    'pipeline_producer_batch_duration_seconds',
    'Time to produce a batch of messages',
    ['topic'],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
)

MESSAGE_SIZE = Histogram(
    'pipeline_producer_message_size_bytes',
    'Size of produced messages in bytes',
    ['topic'],
    buckets=(100, 500, 1000, 2500, 5000, 10000, 25000)
)

ORDERS_BY_CATEGORY = Counter(
    'pipeline_business_orders_total',
    'Total orders by category',
    ['category']
)

REVENUE = Counter(
    'pipeline_business_revenue_total',
    'Total revenue generated',
    ['category']
)

ORDER_VALUE = Histogram(
    'pipeline_business_order_value',
    'Distribution of order values',
    ['category'],
    buckets=(10, 25, 50, 100, 250, 500, 1000, 2500, 5000)
)

ITEMS_PER_ORDER = Histogram(
    'pipeline_business_items_per_order',
    'Distribution of items per order',
    buckets=(1, 2, 3, 4, 5, 10)
)

PRODUCER_UP = Gauge(
    'pipeline_producer_up',
    'Producer is running (1) or not (0)'
)

KAFKA_CONNECTION_STATUS = Gauge(
    'pipeline_producer_kafka_connected',
    'Kafka connection status (1=connected, 0=disconnected)'
)

CURRENT_BATCH_SIZE = Gauge(
    'pipeline_producer_current_batch_size',
    'Current configured batch size'
)

LAST_PRODUCE_TIMESTAMP = Gauge(
    'pipeline_producer_last_produce_timestamp',
    'Timestamp of last successful produce'
)

# ===========================================
# Fake Data Generator
# ===========================================

fake = Faker()

PRODUCTS = {
    "Electronics": [
        {"name": "Smartphone", "price_range": (299, 1299)},
        {"name": "Laptop", "price_range": (499, 2499)},
        {"name": "Tablet", "price_range": (199, 999)},
        {"name": "Headphones", "price_range": (29, 399)},
        {"name": "Smart Watch", "price_range": (99, 599)},
    ],
    "Clothing": [
        {"name": "T-Shirt", "price_range": (15, 45)},
        {"name": "Jeans", "price_range": (35, 120)},
        {"name": "Jacket", "price_range": (50, 250)},
        {"name": "Sneakers", "price_range": (45, 180)},
        {"name": "Dress", "price_range": (30, 150)},
    ],
    "Home & Garden": [
        {"name": "Coffee Maker", "price_range": (25, 200)},
        {"name": "Vacuum Cleaner", "price_range": (80, 400)},
        {"name": "Bed Sheets", "price_range": (30, 120)},
        {"name": "Plant Pot", "price_range": (10, 50)},
        {"name": "Lamp", "price_range": (20, 150)},
    ],
    "Books": [
        {"name": "Fiction Novel", "price_range": (8, 25)},
        {"name": "Technical Book", "price_range": (30, 80)},
        {"name": "Cookbook", "price_range": (15, 45)},
        {"name": "Biography", "price_range": (12, 35)},
        {"name": "Children Book", "price_range": (5, 20)},
    ],
    "Sports": [
        {"name": "Yoga Mat", "price_range": (15, 60)},
        {"name": "Dumbbell Set", "price_range": (30, 150)},
        {"name": "Running Shoes", "price_range": (60, 200)},
        {"name": "Bicycle", "price_range": (200, 1500)},
        {"name": "Tennis Racket", "price_range": (25, 200)},
    ],
}


def generate_order(trace_id: str = None) -> dict[str, Any]:
    """Generate a fake e-commerce order"""
    category = random.choice(list(PRODUCTS.keys()))
    product = random.choice(PRODUCTS[category])
    
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(*product["price_range"]), 2)
    total_amount = round(quantity * unit_price, 2)
    
    order = {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{fake.random_number(digits=6, fix_len=True)}",
        "product_id": f"PROD-{fake.random_number(digits=8, fix_len=True)}",
        "product_name": product["name"],
        "category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "order_status": "pending",
        "customer_email": fake.email(),
        "shipping_address": fake.address().replace("\n", ", "),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    
    # Add trace_id for correlation
    if trace_id:
        order["trace_id"] = trace_id
    
    return order


# ===========================================
# Kafka Producer
# ===========================================

class OrderProducer:
    def __init__(self):
        self.producer = None
        self._connect()
    
    def _connect(self):
        """Connect to Kafka with retry logic"""
        max_retries = 30
        retry_interval = 2
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                )
                logger.info(
                    "Connected to Kafka",
                    extra={
                        "event": "kafka_connected",
                        "bootstrap_servers": settings.kafka_bootstrap_servers,
                        "attempt": attempt + 1
                    }
                )
                PRODUCER_UP.set(1)
                KAFKA_CONNECTION_STATUS.set(1)
                return
            except KafkaError as e:
                KAFKA_CONNECTION_STATUS.set(0)
                ERRORS.labels(error_type="connection", stage="init").inc()
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
    
    def produce(self, order: dict) -> bool:
        """Produce a single order to Kafka with tracing"""
        category = order.get("category", "unknown")
        
        # Create span for this operation
        with tracer.start_as_current_span(
            "produce_order",
            kind=trace.SpanKind.PRODUCER
        ) as span:
            # Add span attributes
            span.set_attribute("messaging.system", "kafka")
            span.set_attribute("messaging.destination", settings.kafka_topic)
            span.set_attribute("order.id", order["order_id"])
            span.set_attribute("order.category", category)
            span.set_attribute("order.amount", order["total_amount"])
            
            # Get trace_id and add to order for correlation
            trace_id = format(span.get_span_context().trace_id, '032x')
            order["trace_id"] = trace_id
            
            message_bytes = json.dumps(order).encode('utf-8')
            span.set_attribute("messaging.message.payload_size_bytes", len(message_bytes))
            
            try:
                start_time = time.time()
                
                future = self.producer.send(
                    settings.kafka_topic,
                    key=order["order_id"],
                    value=order
                )
                future.get(timeout=10)
                
                duration = time.time() - start_time
                
                # Record success in span
                span.set_status(Status(StatusCode.OK))
                span.set_attribute("duration_ms", round(duration * 1000, 2))
                
                # Metrics
                MESSAGES_PRODUCED.labels(
                    topic=settings.kafka_topic,
                    status="success",
                    category=category
                ).inc()
                
                MESSAGE_PRODUCE_DURATION.labels(topic=settings.kafka_topic).observe(duration)
                MESSAGE_SIZE.labels(topic=settings.kafka_topic).observe(len(message_bytes))
                
                ORDERS_BY_CATEGORY.labels(category=category).inc()
                REVENUE.labels(category=category).inc(order["total_amount"])
                ORDER_VALUE.labels(category=category).observe(order["total_amount"])
                ITEMS_PER_ORDER.observe(order["quantity"])
                
                LAST_PRODUCE_TIMESTAMP.set(time.time())
                
                logger.debug(
                    "Order produced successfully",
                    extra={
                        "event": "order_produced",
                        "order_id": order["order_id"],
                        "trace_id": trace_id,
                        "category": category,
                        "amount": order["total_amount"],
                        "duration_ms": round(duration * 1000, 2)
                    }
                )
                
                return True
                
            except Exception as e:
                # Record error in span
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                
                MESSAGES_PRODUCED.labels(
                    topic=settings.kafka_topic,
                    status="failed",
                    category=category
                ).inc()
                
                ERRORS.labels(
                    error_type=type(e).__name__,
                    stage="produce"
                ).inc()
                
                logger.error(
                    "Failed to produce order",
                    extra={
                        "event": "produce_error",
                        "order_id": order.get("order_id"),
                        "trace_id": trace_id,
                        "error": str(e),
                        "error_type": type(e).__name__
                    }
                )
                return False
    
    def produce_batch(self, batch_size: int) -> tuple[int, int]:
        """Produce a batch of orders with tracing"""
        
        # Create parent span for batch
        with tracer.start_as_current_span(
            "produce_batch",
            kind=trace.SpanKind.INTERNAL
        ) as batch_span:
            batch_span.set_attribute("batch.size", batch_size)
            
            success_count = 0
            failed_count = 0
            batch_orders = []
            
            CURRENT_BATCH_SIZE.set(batch_size)
            
            start_time = time.time()
            
            for _ in range(batch_size):
                order = generate_order()
                if self.produce(order):
                    success_count += 1
                    batch_orders.append(order["order_id"])
                else:
                    failed_count += 1
            
            self.producer.flush()
            
            duration = time.time() - start_time
            
            # Record batch results in span
            batch_span.set_attribute("batch.success_count", success_count)
            batch_span.set_attribute("batch.failed_count", failed_count)
            batch_span.set_attribute("batch.duration_ms", round(duration * 1000, 2))
            
            if failed_count == 0:
                batch_span.set_status(Status(StatusCode.OK))
                BATCHES_PRODUCED.labels(status="success").inc()
            elif success_count == 0:
                batch_span.set_status(Status(StatusCode.ERROR, "All messages failed"))
                BATCHES_PRODUCED.labels(status="failed").inc()
            else:
                batch_span.set_status(Status(StatusCode.OK))
                BATCHES_PRODUCED.labels(status="partial").inc()
            
            BATCH_PRODUCE_DURATION.labels(topic=settings.kafka_topic).observe(duration)
            
            logger.info(
                "Batch produced",
                extra={
                    "event": "batch_produced",
                    "batch_size": batch_size,
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "duration_ms": round(duration * 1000, 2),
                    "throughput_per_sec": round(batch_size / duration, 2) if duration > 0 else 0
                }
            )
            
            return success_count, failed_count
    
    def close(self):
        """Close the producer"""
        if self.producer:
            self.producer.close()
            PRODUCER_UP.set(0)
            KAFKA_CONNECTION_STATUS.set(0)
            logger.info("Producer closed", extra={"event": "producer_closed"})


# ===========================================
# Main Loop
# ===========================================

def main():
    logger.info(
        "Starting Data Producer",
        extra={
            "event": "producer_starting",
            "config": {
                "kafka_servers": settings.kafka_bootstrap_servers,
                "topic": settings.kafka_topic,
                "batch_size": settings.batch_size,
                "interval_seconds": settings.produce_interval_seconds,
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
    
    # Create producer
    producer = OrderProducer()
    
    try:
        batch_number = 0
        while True:
            batch_number += 1
            
            # Create span for each iteration
            with tracer.start_as_current_span(f"batch_iteration_{batch_number}"):
                success, failed = producer.produce_batch(settings.batch_size)
            
            time.sleep(settings.produce_interval_seconds)
            
    except KeyboardInterrupt:
        logger.info("Shutdown requested", extra={"event": "shutdown_requested"})
    except Exception as e:
        ERRORS.labels(error_type="fatal", stage="main").inc()
        logger.error(
            "Fatal error in producer",
            extra={"event": "fatal_error", "error": str(e), "error_type": type(e).__name__}
        )
    finally:
        producer.close()
        logger.info("Producer stopped", extra={"event": "producer_stopped"})


if __name__ == "__main__":
    main()
