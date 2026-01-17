# stress-testing/load-test/load_test.py

import json
import random
import time
import uuid
import threading
import signal
import sys
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import click
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# ===========================================
# Metrics
# ===========================================
MESSAGES_SENT = Counter(
    'loadtest_messages_sent_total',
    'Total messages sent',
    ['status']
)

SEND_DURATION = Histogram(
    'loadtest_send_duration_seconds',
    'Time to send message',
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
)

CURRENT_RPS = Gauge(
    'loadtest_current_rps',
    'Current requests per second'
)

ACTIVE_THREADS = Gauge(
    'loadtest_active_threads',
    'Number of active threads'
)

TARGET_RPS = Gauge(
    'loadtest_target_rps',
    'Target requests per second'
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


def generate_order() -> dict[str, Any]:
    """Generate a fake order"""
    category = random.choice(list(PRODUCTS.keys()))
    product = random.choice(PRODUCTS[category])
    
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(*product["price_range"]), 2)
    total_amount = round(quantity * unit_price, 2)
    
    return {
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
        "load_test": True,
    }


# ===========================================
# Load Tester
# ===========================================
class LoadTester:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.running = False
        self.stats = {
            "sent": 0,
            "failed": 0,
            "start_time": None,
        }
        self._connect()
    
    def _connect(self):
        """Connect to Kafka"""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks=1,  # Faster for load testing
                    linger_ms=5,
                    batch_size=16384,
                    buffer_memory=33554432,
                )
                click.echo(f"✅ Connected to Kafka: {self.bootstrap_servers}")
                return
            except KafkaError as e:
                click.echo(f"⏳ Kafka connection attempt {attempt + 1}/{max_retries}: {e}")
                time.sleep(2)
        
        raise Exception("Failed to connect to Kafka")
    
    def send_message(self) -> bool:
        """Send a single message"""
        order = generate_order()
        
        try:
            start = time.time()
            future = self.producer.send(
                self.topic,
                key=order["order_id"],
                value=order
            )
            future.get(timeout=10)
            duration = time.time() - start
            
            MESSAGES_SENT.labels(status="success").inc()
            SEND_DURATION.observe(duration)
            self.stats["sent"] += 1
            return True
            
        except Exception as e:
            MESSAGES_SENT.labels(status="failed").inc()
            self.stats["failed"] += 1
            return False
    
    def run_constant_load(self, rps: int, duration_seconds: int, threads: int = 10):
        """Run constant load test"""
        self.running = True
        self.stats["start_time"] = time.time()
        
        TARGET_RPS.set(rps)
        ACTIVE_THREADS.set(threads)
        
        click.echo(f"\n🚀 Starting Constant Load Test")
        click.echo(f"   Target RPS: {rps}")
        click.echo(f"   Duration: {duration_seconds}s")
        click.echo(f"   Threads: {threads}")
        click.echo("-" * 50)
        
        interval = 1.0 / rps if rps > 0 else 1
        end_time = time.time() + duration_seconds
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            while self.running and time.time() < end_time:
                batch_start = time.time()
                futures = []
                
                # Submit batch of requests
                for _ in range(min(rps, 100)):
                    if not self.running:
                        break
                    futures.append(executor.submit(self.send_message))
                
                # Wait for completion
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception:
                        pass
                
                # Calculate actual RPS
                elapsed = time.time() - self.stats["start_time"]
                actual_rps = self.stats["sent"] / elapsed if elapsed > 0 else 0
                CURRENT_RPS.set(actual_rps)
                
                # Print progress
                remaining = int(end_time - time.time())
                click.echo(
                    f"\r⏱️  Remaining: {remaining:3d}s | "
                    f"Sent: {self.stats['sent']:,} | "
                    f"Failed: {self.stats['failed']:,} | "
                    f"RPS: {actual_rps:.1f}",
                    nl=False
                )
                
                # Rate limiting
                batch_duration = time.time() - batch_start
                sleep_time = max(0, 1.0 - batch_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)
        
        self.producer.flush()
        self._print_summary()
    
    def run_ramp_up(self, start_rps: int, end_rps: int, duration_seconds: int, threads: int = 20):
        """Run ramp-up load test"""
        self.running = True
        self.stats["start_time"] = time.time()
        
        ACTIVE_THREADS.set(threads)
        
        click.echo(f"\n📈 Starting Ramp-Up Load Test")
        click.echo(f"   Start RPS: {start_rps}")
        click.echo(f"   End RPS: {end_rps}")
        click.echo(f"   Duration: {duration_seconds}s")
        click.echo(f"   Threads: {threads}")
        click.echo("-" * 50)
        
        rps_increment = (end_rps - start_rps) / duration_seconds
        end_time = time.time() + duration_seconds
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            while self.running and time.time() < end_time:
                elapsed = time.time() - self.stats["start_time"]
                current_target_rps = int(start_rps + (rps_increment * elapsed))
                TARGET_RPS.set(current_target_rps)
                
                batch_start = time.time()
                futures = []
                
                for _ in range(min(current_target_rps, 200)):
                    if not self.running:
                        break
                    futures.append(executor.submit(self.send_message))
                
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception:
                        pass
                
                actual_rps = self.stats["sent"] / elapsed if elapsed > 0 else 0
                CURRENT_RPS.set(actual_rps)
                
                remaining = int(end_time - time.time())
                click.echo(
                    f"\r⏱️  Remaining: {remaining:3d}s | "
                    f"Target RPS: {current_target_rps:3d} | "
                    f"Actual RPS: {actual_rps:.1f} | "
                    f"Sent: {self.stats['sent']:,}",
                    nl=False
                )
                
                batch_duration = time.time() - batch_start
                sleep_time = max(0, 1.0 - batch_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)
        
        self.producer.flush()
        self._print_summary()
    
    def run_spike(self, base_rps: int, spike_rps: int, spike_duration: int, total_duration: int, threads: int = 20):
        """Run spike test"""
        self.running = True
        self.stats["start_time"] = time.time()
        
        ACTIVE_THREADS.set(threads)
        
        click.echo(f"\n⚡ Starting Spike Test")
        click.echo(f"   Base RPS: {base_rps}")
        click.echo(f"   Spike RPS: {spike_rps}")
        click.echo(f"   Spike Duration: {spike_duration}s")
        click.echo(f"   Total Duration: {total_duration}s")
        click.echo("-" * 50)
        
        end_time = time.time() + total_duration
        spike_start = time.time() + (total_duration - spike_duration) / 2
        spike_end = spike_start + spike_duration
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            while self.running and time.time() < end_time:
                current_time = time.time()
                
                # Determine current RPS
                if spike_start <= current_time <= spike_end:
                    current_rps = spike_rps
                    phase = "🔥 SPIKE"
                else:
                    current_rps = base_rps
                    phase = "📊 BASE "
                
                TARGET_RPS.set(current_rps)
                
                batch_start = time.time()
                futures = []
                
                for _ in range(min(current_rps, 200)):
                    if not self.running:
                        break
                    futures.append(executor.submit(self.send_message))
                
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception:
                        pass
                
                elapsed = time.time() - self.stats["start_time"]
                actual_rps = self.stats["sent"] / elapsed if elapsed > 0 else 0
                CURRENT_RPS.set(actual_rps)
                
                remaining = int(end_time - time.time())
                click.echo(
                    f"\r{phase} | Remaining: {remaining:3d}s | "
                    f"RPS: {actual_rps:.1f} | "
                    f"Sent: {self.stats['sent']:,}",
                    nl=False
                )
                
                batch_duration = time.time() - batch_start
                sleep_time = max(0, 1.0 - batch_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)
        
        self.producer.flush()
        self._print_summary()
    
    def _print_summary(self):
        """Print test summary"""
        elapsed = time.time() - self.stats["start_time"]
        total = self.stats["sent"] + self.stats["failed"]
        success_rate = (self.stats["sent"] / total * 100) if total > 0 else 0
        avg_rps = self.stats["sent"] / elapsed if elapsed > 0 else 0
        
        click.echo("\n")
        click.echo("=" * 50)
        click.echo("📊 LOAD TEST SUMMARY")
        click.echo("=" * 50)
        click.echo(f"   Duration:     {elapsed:.1f}s")
        click.echo(f"   Total Sent:   {self.stats['sent']:,}")
        click.echo(f"   Failed:       {self.stats['failed']:,}")
        click.echo(f"   Success Rate: {success_rate:.2f}%")
        click.echo(f"   Avg RPS:      {avg_rps:.1f}")
        click.echo("=" * 50)
    
    def stop(self):
        """Stop the test"""
        self.running = False
        if self.producer:
            self.producer.close()


# ===========================================
# CLI
# ===========================================
@click.group()
def cli():
    """Load Testing Tool for Data Pipeline"""
    pass


@cli.command()
@click.option('--rps', default=50, help='Requests per second')
@click.option('--duration', default=60, help='Duration in seconds')
@click.option('--threads', default=10, help='Number of threads')
@click.option('--kafka', default='kafka:29092', help='Kafka bootstrap servers')
@click.option('--topic', default='ecommerce.orders', help='Kafka topic')
@click.option('--metrics-port', default=8002, help='Prometheus metrics port')
def constant(rps, duration, threads, kafka, topic, metrics_port):
    """Run constant load test"""
    start_http_server(metrics_port)
    click.echo(f"📊 Metrics available at http://localhost:{metrics_port}/metrics")
    
    tester = LoadTester(kafka, topic)
    
    def signal_handler(sig, frame):
        click.echo("\n\n⚠️  Stopping test...")
        tester.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    tester.run_constant_load(rps, duration, threads)


@cli.command()
@click.option('--start-rps', default=10, help='Starting RPS')
@click.option('--end-rps', default=100, help='Ending RPS')
@click.option('--duration', default=120, help='Duration in seconds')
@click.option('--threads', default=20, help='Number of threads')
@click.option('--kafka', default='kafka:29092', help='Kafka bootstrap servers')
@click.option('--topic', default='ecommerce.orders', help='Kafka topic')
@click.option('--metrics-port', default=8002, help='Prometheus metrics port')
def rampup(start_rps, end_rps, duration, threads, kafka, topic, metrics_port):
    """Run ramp-up load test"""
    start_http_server(metrics_port)
    click.echo(f"📊 Metrics available at http://localhost:{metrics_port}/metrics")
    
    tester = LoadTester(kafka, topic)
    
    def signal_handler(sig, frame):
        click.echo("\n\n⚠️  Stopping test...")
        tester.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    tester.run_ramp_up(start_rps, end_rps, duration, threads)


@cli.command()
@click.option('--base-rps', default=20, help='Base RPS')
@click.option('--spike-rps', default=200, help='Spike RPS')
@click.option('--spike-duration', default=30, help='Spike duration in seconds')
@click.option('--total-duration', default=120, help='Total duration in seconds')
@click.option('--threads', default=20, help='Number of threads')
@click.option('--kafka', default='kafka:29092', help='Kafka bootstrap servers')
@click.option('--topic', default='ecommerce.orders', help='Kafka topic')
@click.option('--metrics-port', default=8002, help='Prometheus metrics port')
def spike(base_rps, spike_rps, spike_duration, total_duration, threads, kafka, topic, metrics_port):
    """Run spike test"""
    start_http_server(metrics_port)
    click.echo(f"📊 Metrics available at http://localhost:{metrics_port}/metrics")
    
    tester = LoadTester(kafka, topic)
    
    def signal_handler(sig, frame):
        click.echo("\n\n⚠️  Stopping test...")
        tester.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    tester.run_spike(base_rps, spike_rps, spike_duration, total_duration, threads)


if __name__ == "__main__":
    cli()
