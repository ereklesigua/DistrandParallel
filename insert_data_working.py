
from cassandra.cluster import Cluster
import uuid
from datetime import datetime, timedelta
import random


try:
    cluster = Cluster(['localhost'])
    session = cluster.connect('sensor_data')
except Exception as e:
    print("Error connecting to Cassandra:", e)
    exit(1)


def insert_simulated_data(session, num_records=50):
    base_temp = 22.0
    base_humidity = 60.0
    base_solar = 750.0

    for i in range(num_records):
        timestamp = datetime.utcnow() + timedelta(minutes=i)
        location = 'Berlin'
        temperature = round(base_temp + random.uniform(-2, 2), 2)
        humidity = round(base_humidity + random.uniform(-5, 5), 2)
        solar_irradiation = round(base_solar + random.uniform(-50, 50), 2)
        data_source = 'sensor_001'

        temp_diff_prev = round(temperature - base_temp, 2) if i > 0 else 0.0
        humidity_diff_prev = round(humidity - base_humidity, 2) if i > 0 else 0.0
        solar_irradiation_diff_prev = round(solar_irradiation - base_solar, 2) if i > 0 else 0.0

    
        session.execute("""
            INSERT INTO environmental_readings (
                id, timestamp, location, temperature, humidity,
                solar_irradiation, temp_diff_prev, humidity_diff_prev,
                solar_irradiation_diff_prev, data_source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [
            uuid.uuid4(),  
            timestamp,
            location,
            temperature,
            humidity,
            solar_irradiation,
            temp_diff_prev,
            humidity_diff_prev,
            solar_irradiation_diff_prev,
            data_source
        ])

    print(f"{num_records} records inserted successfully!")

if __name__ == "__main__":
    insert_simulated_data(session)