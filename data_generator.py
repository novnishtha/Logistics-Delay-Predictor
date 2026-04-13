import random

def generate_logistics_event():
    distance = round(random.uniform(10, 500), 2)
    speed = random.uniform(40, 90)
    weather = random.choice(['Clear', 'Rainy', 'Stormy'])
    traffic = random.choice(['Low', 'Moderate', 'Heavy'])

    delay = random.uniform(0, 10)

    if weather == 'Stormy':
        delay += 20
    elif weather == 'Rainy':
        delay += 10

    if traffic == 'Heavy':
        delay += 15
    elif traffic == 'Moderate':
        delay += 7

    if weather == 'Stormy' and traffic == 'Heavy':
        delay += 10

    delay -= (speed / 30)
    delay += (distance/100)*2
    delay = max(0, min(delay, 60))

    return {
        "distance_km": distance,
        "speed_kmh": round(speed, 2),
        "weather": weather,
        "traffic": traffic,
        "delay_minutes": delay
    }