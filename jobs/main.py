import os
from confluent_kafka import Producer, SerializingProducer
import simplejson as json
import random
import time
import uuid
from datetime import datetime, timedelta

DELHI_COORDINATES = {"latitude": 28.6139, "longitude": 77.2090}
GURGAON_COORDINATES = {"latitude": 28.4595, "longitude": 77.0266}

# Calculate movement increments
LATITUDE_INCREMENT = (GURGAON_COORDINATES["latitude"] - DELHI_COORDINATES["latitude"])/100
LONGITUDE_INCREMENT = (GURGAON_COORDINATES["longitude"] - DELHI_COORDINATES["longitude"])/100

# Environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', "localhost:9092")
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

random.seed(37)

start_time = datetime.now()
start_location = DELHI_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60)) # update freuency
    return start_time

def generate_gps_data(device_id, timestamp, vehile_type='private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40), # km/h
        'direction': 'North-East',
        'vehicleType': vehile_type
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(28, 40),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rainy', 'Snow']),
        'precipitatoin': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(100, 200)
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of incident'
    }

def json_serialize(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery Failed: {err}")
    else:
        print(f"Message Delivered to {msg.topic()} [{msg.partition()}]")

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic, 
        key=str(data['id']), 
        value=json.dumps(data, default=json_serialize).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()

def simulate_vehicle_movement():
    global start_location
    # move towards gurgaon
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # add some randomness to simulate actual road travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed':random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }

def simulate_journey(producer, device_id):
    while True: # Driver still drives
        vehice_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehice_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehice_data['timestamp'], vehice_data['location'], 'Nikon-R34')
        weather_data = generate_weather_data(device_id, vehice_data['timestamp'], vehice_data['location'])
        emergency_data = generate_emergency_incident_data(device_id, vehice_data['timestamp'], vehice_data['location'])

        if (vehice_data['location'][0] <= GURGAON_COORDINATES['latitude'] and vehice_data['location'][1] <= GURGAON_COORDINATES['longitude']):
            print("Vehicle has reached Gurgaon. Simulation Ended...")
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehice_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)
        time.sleep(3)

if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f"Kafka error: {err}")
    }
    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, 'Vehicle-A-234')
    except KeyboardInterrupt:
        print('Simulation ended by user.')
    except Exception as e:
        print('f"Unexpected error occured: {e}')
