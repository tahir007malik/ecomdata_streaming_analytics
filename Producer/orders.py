from confluent_kafka import Producer
import json
import time
import random
from datetime import datetime

# Configuration for the Kafka producer
producer_config = {
    'bootstrap.servers': 'pkc-4rn2p.canadacentral.azure.confluent.cloud:9092',  # Replace with your Kafka broker URL
    'security.protocol': 'SASL_SSL',     # For Confluent Cloud
    'sasl.mechanism': 'PLAIN',           # For Confluent Cloud
    'sasl.username': 'NS53BKJKZZW2XKVD',  # Replace with your API key
    'sasl.password': 'k6XkTWqYkdNAoX6J93TOmZQhZulWAAAsonN7PZEfulBvc+WKRGsGO03RhiMbwhe2',  # Replace with your API secret
}

# Initialize producer
producer = Producer(producer_config)

# Topic name
topic = 'orders'  # Replace with your topic name

# Function to simulate random order data
def generate_order_data(order_id):
    user_id = f'user_{random.randint(1, 100000)}'
    items = [
        {
            "product_id": f"prod_{random.randint(1, 1000)}",
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(10, 100), 2)
        }
        for _ in range(random.randint(1, 5))  # Random number of items in the order
    ]
    total_amount = sum(item['quantity'] * item['price'] for item in items)
    return {
        "order_id": f"order_{order_id}",
        "user_id": user_id,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "items": items,
        "total_amount": round(total_amount, 2),
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "cash_on_delivery"]),
        "status": random.choice(["completed", "pending", "canceled"]),
    }

# Function to send events to Kafka
def produce_orders():
    try:
        order_id = 1
        while True:
            order_data = generate_order_data(order_id)
            producer.produce(topic, key=f"{order_data['order_id']}", value=json.dumps(order_data))
            print(f"Produced order: {order_data}")
            
            order_id += 1
            if order_id % 10 == 0:  # Flush every 10 messages
                producer.flush()
            
            time.sleep(.5)  # Simulate real-time generation
    except KeyboardInterrupt:
        print("Stopped producing orders.")
    except BufferError:
        print("Producer queue is full. Retrying...")
        producer.flush()
    except Exception as e:
        print(f"Failed to produce message: {e}")
    finally:
        producer.flush()  # Ensure all messages are sent before exiting
        producer.close()

# Run the producer
if __name__ == "__main__":
    produce_orders()
