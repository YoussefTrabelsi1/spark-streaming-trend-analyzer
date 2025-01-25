import requests
from kafka import KafkaProducer
import json
import time

# Kafka Configuration
KAFKA_TOPIC = 'weather-topic'
KAFKA_BROKER = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Cities to fetch weather data for
CITIES = ['London', 'Paris', 'New York', 'Tokyo', 'Sydney']
URL_TEMPLATE = "http://wttr.in/{city}?format=j1"  # wttr.in JSON format

def fetch_weather():
    while True:
        for city in CITIES:
            try:
                # Request weather data for the city
                response = requests.get(URL_TEMPLATE.format(city=city))
                if response.status_code == 200:
                    data = response.json()

                    # Extract key information
                    processed_data = {
                        "city": city,
                        "temperature": data["current_condition"][0]["temp_C"],
                        "humidity": data["current_condition"][0]["humidity"],
                        "weather_desc": data["current_condition"][0]["weatherDesc"][0]["value"],
                        "wind_speed_kmph": data["current_condition"][0]["windspeedKmph"],
                        "visibility_km": data["current_condition"][0]["visibility"],
                        "feels_like_C": data["current_condition"][0]["FeelsLikeC"],
                        "observation_time": data["current_condition"][0]["observation_time"]
                    }

                    # Send the data to Kafka
                    producer.send(KAFKA_TOPIC, processed_data)
                    print(f"Sent: {processed_data}")
                else:
                    print(f"Error fetching data for {city}: {response.status_code}")
            except Exception as e:
                print(f"Error processing city {city}: {e}")

        # Wait for a minute before fetching again
        time.sleep(60)

if __name__ == "__main__":
    fetch_weather()
