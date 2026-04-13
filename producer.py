from kafka import KafkaProducer
import json
import random
import time

# Fixed fleet
vehicles = [f"TRUCK_{i}" for i in range(100, 130)]

vehicle_state = {
    v: {
        "distance": random.uniform(100, 200),
        "delay": random.uniform(0, 5)
    } for v in vehicles
}

# Generate ONE truck event
def generate_logistics_event(vehicle_id):

    state = vehicle_state[vehicle_id]

    # Speed
    speed = random.uniform(40, 90)

    # 1-minute simulation
    distance_travelled = speed / 60
    state["distance"] -= distance_travelled

    # Reset trip
    if state["distance"] <= 0:
        state["distance"] = 0
        state["completed"] = True
    else:
        state["completed"] = False

    if state["distance"] <= 0:
        state["distance"] = 0
        state["completed"] = True
    else:
        state["completed"] = False

    distance = round(state["distance"], 2)

    # Conditions
    weather = random.choice(['Clear', 'Rainy', 'Stormy'])
    traffic = random.choice(['Low', 'Moderate', 'Heavy'])

    delay = state["delay"]

    if weather == 'Stormy':
        delay += random.uniform(2, 5)
    elif weather == 'Rainy':
        delay += random.uniform(1, 3)

    if traffic == 'Heavy':
        delay += random.uniform(2, 6)

    delay -= (speed / 30)
    delay = max(0, min(delay, 60))

    state["delay"] = delay

    return {
        "timestamp": time.time(),
        "vehicle_id": vehicle_id,
        "distance_km": distance,
        "speed_kmh": round(speed, 2),
        "weather": weather,
        "traffic": traffic,
        "delay_minutes": round(delay, 2)
    }

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic_name = 'logistics_events'

print(f"Starting stream to topic: {topic_name}...")

try:
    while True:

        for vehicle_id in vehicles:
            event = generate_logistics_event(vehicle_id)

            producer.send(
                topic_name,
                key=vehicle_id.encode('utf-8'),
                value=event
            )

            print(f"[SEND] {vehicle_id} | Dist: {event['distance_km']} | Delay: {event['delay_minutes']}")

        time.sleep(3)  # 1 sec = 1 min simulated

except KeyboardInterrupt:
    print("Streaming stopped.")
finally:
    producer.flush()