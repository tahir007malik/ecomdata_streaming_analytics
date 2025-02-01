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
topic = 'user-profiles'  # Replace with your topic name

# Function to simulate random user profile updates
def generate_user_profile_event():
    actions = ['profile_creation', 'profile_update', 'password_reset', 'email_verification']
    categories = ['electronics', 'fashion', 'home_appliances', 'sports', 'beauty']
    
    return {
        "user_id": f"user_{random.randint(1, 100000)}",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "action": random.choice(actions),
        "details": {
            "email": f"user{random.randint(1, 100000)}@example.com",
            "name": f"User{random.randint(1, 100000)}",
            "preferred_category": random.choice(categories)
        }
    }

# Function to send events to Kafka
def produce_user_profiles():
    try:
        while True:
            user_event = generate_user_profile_event()
            producer.produce(topic, key=f"{user_event['user_id']}", value=json.dumps(user_event))
            print(f"Produced user profile event: {user_event}")
            
            producer.flush()  # Ensure the message is sent
            time.sleep(.5)  # Simulate real-time generation
    except KeyboardInterrupt:
        print("Stopped producing user profile events.")
    finally:
        producer.flush()  # Ensure all messages are sent before exiting
        producer.close()

# Run the producer
if __name__ == "__main__":
    produce_user_profiles()
