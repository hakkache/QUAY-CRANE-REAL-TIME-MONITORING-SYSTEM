import json
import time
import random
import signal
import sys
from datetime import datetime
from multiprocessing import Process, Value, cpu_count
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from QuayCraneSensorSimulator import QuayCraneSensorSimulator


# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092,localhost:39092,localhost:49092'
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3

# High throughput settings
NUM_PRODUCER_PROCESSES = 12  # Number of parallel producer processes
BATCH_SIZE_PER_CRANE = 50    # Messages to generate per crane per cycle

# Topics to create
TOPICS = [
    'crane_sensors',      # Raw sensor data from all cranes
    'crane_alerts',       # Alert events (high temp, overload, etc.)
    'crane_performance',  # Performance metrics (cycle times, throughput)
]


def create_kafka_topics():
    """Create Kafka topics with specified partitions and replication factor."""
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    
    # Check existing topics
    existing_topics = admin_client.list_topics(timeout=10).topics.keys()
    
    # Define new topics
    new_topics = []
    for topic_name in TOPICS:
        if topic_name not in existing_topics:
            new_topics.append(NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            ))
            print(f"📝 Will create topic: {topic_name} (partitions={NUM_PARTITIONS}, replicas={REPLICATION_FACTOR})")
        else:
            print(f"✅ Topic already exists: {topic_name}")
    
    # Create topics
    if new_topics:
        futures = admin_client.create_topics(new_topics)
        
        for topic_name, future in futures.items():
            try:
                future.result()  # Wait for topic creation
                print(f"✅ Created topic: {topic_name}")
            except Exception as e:
                print(f"❌ Failed to create topic {topic_name}: {e}")
    
    return admin_client


def delivery_callback(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    # Uncomment for verbose logging:
    # else:
    #     print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")


def create_producer():
    """Create and configure Kafka producer for high throughput."""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': f'quay-crane-producer-{random.randint(1000,9999)}',
        'acks': '1',  # Only leader ack for speed
        'retries': 0,
        'batch.num.messages': 10000,
        'linger.ms': 5,
        'queue.buffering.max.messages': 500000,
        'queue.buffering.max.kbytes': 1048576,
        'compression.type': 'lz4',
    }
    return Producer(config)


def publish_sensor_data(producer, sensor_data):
    """Publish sensor reading to appropriate Kafka topics (no callbacks for speed)."""
    crane_id = sensor_data['crane_id']
    message_key = crane_id.encode('utf-8')
    
    # 1. Publish to main sensor topic
    producer.produce(
        topic='crane_sensors',
        key=message_key,
        value=json.dumps(sensor_data).encode('utf-8')
    )
    
    # 2. Publish alerts
    if sensor_data['alert_count'] > 0:
        alert_message = {
            'crane_id': crane_id,
            'timestamp': sensor_data['timestamp'],
            'datetime': sensor_data['datetime'],
            'alerts': sensor_data['alerts'],
            'alert_count': sensor_data['alert_count'],
            'motor_temperature_c': sensor_data['motor_temperature_c'],
            'load_percentage': sensor_data['load_percentage'],
            'health_score': sensor_data['health_score'],
        }
        producer.produce(
            topic='crane_alerts',
            key=message_key,
            value=json.dumps(alert_message).encode('utf-8')
        )
    
    # 3. Publish performance metrics
    if sensor_data['operation_type'] in ['loading', 'unloading']:
        performance_message = {
            'crane_id': crane_id,
            'timestamp': sensor_data['timestamp'],
            'datetime': sensor_data['datetime'],
            'operation_type': sensor_data['operation_type'],
            'container_type': sensor_data['container_type'],
            'cycle_time_seconds': sensor_data['cycle_time_seconds'],
            'load_weight_kg': sensor_data['load_weight_kg'],
            'power_consumption_kw': sensor_data['power_consumption_kw'],
        }
        producer.produce(
            topic='crane_performance',
            key=message_key,
            value=json.dumps(performance_message).encode('utf-8')
        )


def producer_worker(process_id, message_counter, running):
    """
    Worker process that generates and publishes sensor data at high speed.
    Each process has its own producer and simulator instance.
    """
    producer = create_producer()
    simulator = QuayCraneSensorSimulator()
    crane_ids = list(simulator.crane_specs.keys())
    local_count = 0
    
    while running.value:
        # Generate batch of messages for each crane
        for crane_id in crane_ids:
            for _ in range(BATCH_SIZE_PER_CRANE):
                # High activity rate
                if random.random() < 0.85:
                    operation = 'loading' if random.random() < 0.6 else 'unloading'
                else:
                    operation = 'idle'
                
                sensor_data = simulator.generate_sensor_reading(crane_id, operation)
                publish_sensor_data(producer, sensor_data)
                local_count += 1
        
        # Poll for callbacks and flush periodically
        producer.poll(0)
        
        # Update shared counter periodically
        if local_count % 1000 == 0:
            with message_counter.get_lock():
                message_counter.value += 1000
            local_count = 0
    
    # Final flush
    producer.flush()


def run_high_throughput_simulation(duration_seconds=None):
    """
    Run high-throughput simulation using multiple processes.
    Target: 100k+ messages per second.
    """
    print("\n" + "="*80)
    print("🚀 HIGH THROUGHPUT QUAY CRANE SENSOR STREAMING")
    print(f"   Target: 100,000+ messages/second")
    print(f"   Producer processes: {NUM_PRODUCER_PROCESSES}")
    print(f"   Batch size per crane: {BATCH_SIZE_PER_CRANE}")
    print(f"   Cranes per process: 6")
    print(f"   Brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    print("="*80 + "\n")
    
    # Shared state
    message_counter = Value('i', 0)
    running = Value('b', True)
    
    # Start producer processes
    processes = []
    for i in range(NUM_PRODUCER_PROCESSES):
        p = Process(target=producer_worker, args=(i, message_counter, running))
        p.start()
        processes.append(p)
        print(f"✅ Started producer process {i+1}/{NUM_PRODUCER_PROCESSES}")
    
    print(f"\n🔥 All {NUM_PRODUCER_PROCESSES} producers running! Ctrl+C to stop.\n")
    
    start_time = time.time()
    last_count = 0
    last_time = start_time
    
    try:
        while True:
            time.sleep(1)
            
            current_time = time.time()
            elapsed = current_time - start_time
            interval = current_time - last_time
            
            current_count = message_counter.value
            interval_count = current_count - last_count
            rate = interval_count / interval if interval > 0 else 0
            avg_rate = current_count / elapsed if elapsed > 0 else 0
            
            print(f"📊 [{elapsed:6.0f}s] Total: {current_count:,} | "
                  f"Rate: {rate:,.0f}/s | Avg: {avg_rate:,.0f}/s")
            
            last_count = current_count
            last_time = current_time
            
            if duration_seconds and elapsed >= duration_seconds:
                break
                
    except KeyboardInterrupt:
        print("\n\n⚠️  Stopping producers...")
    
    finally:
        running.value = False
        
        for p in processes:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
        
        elapsed = time.time() - start_time
        final_count = message_counter.value
        
        print("\n" + "="*80)
        print("📈 FINAL SUMMARY")
        print(f"   Duration: {elapsed:.1f} seconds")
        print(f"   Total messages: {final_count:,}")
        print(f"   Average rate: {final_count/elapsed:,.0f} messages/second")
        print("="*80)


def main():
    """Main entry point."""
    print("\n" + "="*80)
    print("🚢 QUAY CRANE IoT SENSOR DATA PIPELINE - HIGH THROUGHPUT MODE")
    print("   Target: 100,000+ messages/second")
    print("="*80 + "\n")
    
    # Step 1: Create Kafka topics
    print("📡 Setting up Kafka topics...\n")
    try:
        create_kafka_topics()
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        print("   Make sure Kafka brokers are running!")
        sys.exit(1)
    
    # Step 2: Run high-throughput simulation
    print("\n" + "-"*80)
    print("Starting HIGH THROUGHPUT sensor data stream (Ctrl+C to stop)...")
    print("-"*80)
    
    run_high_throughput_simulation(duration_seconds=None)


if __name__ == "__main__":
    main()
