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
topic = 'delivery-status'  # Replace with your topic name

# Function to simulate random delivery status events
def generate_delivery_status_event():
    statuses = ['picked_up', 'in_transit', 'delivered', 'failed', 'returned']
    locations = [
        # Asia
        'Tokyo, Japan', 'Seoul, South Korea', 'Beijing, China', 'Mumbai, India', 'Singapore',
        'Bangkok, Thailand', 'Kuala Lumpur, Malaysia', 'Dubai, UAE', 'Ho Chi Minh City, Vietnam',
        
        # Europe
        'London, UK', 'Paris, France', 'Berlin, Germany', 'Madrid, Spain', 'Rome, Italy',
        'Amsterdam, Netherlands', 'Brussels, Belgium', 'Vienna, Austria', 'Zurich, Switzerland',
        'Stockholm, Sweden',

        # North America
        'New York, USA', 'Los Angeles, USA', 'Toronto, Canada', 'Vancouver, Canada', 'Mexico City, Mexico',
        'Chicago, USA', 'San Francisco, USA', 'Montreal, Canada', 'Boston, USA', 'Atlanta, USA',
        
        # South America
        'Sao Paulo, Brazil', 'Buenos Aires, Argentina', 'Rio de Janeiro, Brazil', 'Lima, Peru',
        'Bogota, Colombia', 'Santiago, Chile', 'Caracas, Venezuela',

        # Africa
        'Cairo, Egypt', 'Lagos, Nigeria', 'Johannesburg, South Africa', 'Nairobi, Kenya', 'Kinshasa, DR Congo',
        'Cape Town, South Africa',

        # Oceania
        'Sydney, Australia', 'Melbourne, Australia', 'Auckland, New Zealand',

        # Middle East
        'Riyadh, Saudi Arabia', 'Tehran, Iran', 'Baghdad, Iraq', 'Doha, Qatar', 'Abu Dhabi, UAE'
    ]
    
    return {
        "order_id": f"order_{random.randint(1, 50000)}",
        "delivery_person_id": f"dp_{random.randint(1, 1000)}",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "status": random.choice(statuses),
        "current_location": random.choice(locations),
    }

# Function to send events to Kafka
def produce_delivery_status():
    try:
        while True:
            delivery_event = generate_delivery_status_event()
            producer.produce(topic, key=f"{delivery_event['order_id']}", value=json.dumps(delivery_event))
            print(f"Produced delivery status event: {delivery_event}")
            
            producer.flush()  # Ensure the message is sent
            time.sleep(0.5)  # Simulate real-time generation
    except KeyboardInterrupt:
        print("Stopped producing delivery status events.")
    finally:
        producer.flush()  # Ensure all messages are sent before exiting
        producer.close()

# Run the producer
if __name__ == "__main__":
    produce_delivery_status()
