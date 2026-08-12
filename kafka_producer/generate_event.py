import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker          # Supporting the multuple locales at a time
from kafka import KafkaProducer  # Class that publishes the bunch of records and information onto the Kafka Cluster

# Faker generates realistic fake data (names, IDs, etc.) — we use it here
# just to add some randomness/realism to our simulated events
fake = Faker()

# The Kafka topic we're publishing to — must match what we create in Kafka
TOPIC = "clickstream-events"

# Kafka is running locally via Docker, exposed on this port (see docker-compose.yml)
BOOTSTRAP_SERVERS = ["localhost:9092"]

# Possible event types a user could trigger while browsing
EVENT_TYPES = ["view", "cart", "remove_from_cart", "purchase"]

# Possible product categories
CATEGORIES = ["electronics", "apparel", "home", "beauty", "sports", "books"]

# Create the Kafka producer connection
# value_serializer converts our Python dict into JSON bytes before sending,
# since Kafka only transmits raw bytes, not Python objects directly
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Create the function for generating the event
def generate_event(session_id: str, user_id: str) -> dict:
    """Builds a single fake clickstream event as a dictionary."""
    category = random.choice(CATEGORIES)
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "event_type": random.choices(EVENT_TYPES, weights=[70, 15, 5, 10])[0],
        "user_id": user_id,
        "session_id": session_id,
        "product_id": f"P{random.randint(1000, 9999)}",
        "category": category,
        "price": round(random.uniform(5, 500), 2),
    }

# create the function for the session_simulation
def simulate_session():
    """Simulates one user's browsing session with multiple events."""
    user_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    num_events = random.randint(1, 12)

    for _ in range(num_events):
        event = generate_event(session_id, user_id)
        producer.send(TOPIC, value=event)
        print(f"Sent: {event['event_type']} | {event['category']} | session={session_id[:8]}")
        time.sleep(random.uniform(0.2, 2.0))


# stored the main into the name function.
if __name__ == "__main__":
    print("Starting clickstream event producer. Press Ctrl+C to stop.")
    try:
        while True:
            simulate_session()
            producer.flush()
    except KeyboardInterrupt:
        print("\nStopping producer.")
        producer.close()

#  To stop the terminal use the shortcut key (ctrl+C)
