from confluent_kafka import Producer
from datetime import datetime
import time
import json
import random

# Configuration for the Kafka producer
producer_config = {
    'bootstrap.servers': 'pkc-4rn2p.canadacentral.azure.confluent.cloud:9092',  # Replace with your Kafka broker URL
    'security.protocol': 'SASL_SSL',     # For Confluent Cloud (optional for local)
    'sasl.mechanism': 'PLAIN',           # For Confluent Cloud (optional for local)
    'sasl.username': 'NS53BKJKZZW2XKVD',        # Replace with your API key if needed
    'sasl.password': 'k6XkTWqYkdNAoX6J93TOmZQhZulWAAAsonN7PZEfulBvc+WKRGsGO03RhiMbwhe2',     # Replace with your API secret if needed
}

# Initialize producer
producer = Producer(producer_config)

# Topic name
topic = 'clickstream'  # Replace with your topic name

# Function to simulate random clickstream data
def generate_clickstream_data():
    events = ['page_view', 'search', 'click', 'add_to_cart', 'checkout']
    pages = ['home_page', 'product_page', 'search_results', 'checkout_page']
    referrers = ['google.com', 'facebook.com', 'email_campaign', 'direct']
    devices = ['mobile', 'desktop', 'tablet']
    
    return {
        'event': random.choice(events),
        'user_id': f'user_{random.randint(1, 100000)}',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'page': random.choice(pages),
        'referrer': random.choice(referrers),
        'device': random.choice(devices)
    }

# Function to send events to Kafka
def produce_events():
    message_count = 0
    try:
        while True:
            event = generate_clickstream_data()
            producer.produce(topic, json.dumps(event))
            print(f"Produced event: {event}")

            message_count = message_count + 1
            if message_count % 10 == 0: # Flush every 10 messages
                producer.flush() # Ensure the message is sent

            time.sleep(.5)  # Simulate real-time generation
    except KeyboardInterrupt:
        print("Stopped producing events.")
    except BufferError:
        print("Producer queue is full. Retrying...")
        producer.flush()
    except Exception as e:
        print(f"Failed to produce message: {e}")
    finally:
        producer.flush() # Ensure all messages are sent before exiting
        producer.close()

# Run the producer
if __name__ == "__main__":
    produce_events()
