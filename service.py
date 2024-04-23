import os
import numpy as np
import json
import datetime
from tensorflow.keras.models import load_model
from joblib import load
from kafka import KafkaConsumer
import http.client

kafka_bridge_password = os.getenv('KAFKA_BRDIGE_PASSWORD')
kafka_password = os.getenv('KAFKA_PASSWORD')

# Kafka HTTP Bridge configuration
KAFKA_HTTP_BRIDGE = 'api.kafka.safewchair.duckdns.org'
ALERTS_TOPIC = 'Alerts'
headers = {
    'content-type': 'application/vnd.kafka.json.v2+json',
    'Authorization': f'Basic {kafka_bridge_password}'
}

# Kafka consumer configuration
KAFKA_BROKERS = 'kafka.safewchair.duckdns.org:31565'
CONSUMER_TOPIC = 'Testing'
SECURITY_PROTOCOL = 'SASL_PLAINTEXT'
SASL_MECHANISM = 'SCRAM-SHA-512'
USERNAME = 'safewchair'
PASSWORD = kafka_password

# Load the model and scaler
model = load_model('model.h5')
scaler = load('scaler.joblib')

consumer = KafkaConsumer(
    CONSUMER_TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
    security_protocol=SECURITY_PROTOCOL,
    sasl_mechanism=SASL_MECHANISM,
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

buffer_coordenadas = []
cooldown_time = None  # Initialize cooldown time

def send_alert(message):
    global cooldown_time
    now = datetime.datetime.now()
    if cooldown_time is None or now >= cooldown_time:
        conn = http.client.HTTPSConnection(KAFKA_HTTP_BRIDGE)
        payload = json.dumps({
            "records": [
                {
                    "key": "fall-detection",
                    "value": message
                }
            ]
        })
        conn.request("POST", f"/topics/{ALERTS_TOPIC}", payload, headers)
        response = conn.getresponse()
        conn.close()
        cooldown_time = now + datetime.timedelta(seconds=10)  # Set cooldown period to 10 seconds
        print(f"Alert sent. Cooldown set until {cooldown_time}.")

for message in consumer:
    coordenadas_str = message.value  # Assuming each message is a string of coordinates separated by commas
    try:
        coordenadas = np.fromstring(coordenadas_str, dtype=float, sep=',')
        buffer_coordenadas.append(coordenadas)
        
        if len(buffer_coordenadas) == 600:
            data = np.array(buffer_coordenadas)
            data_flattened = data.flatten().reshape(1, -1)
            data_scaled = scaler.transform(data_flattened)
            data_scaled = data_scaled.reshape((1, 600, 6))
            
            prediction = model.predict(data_scaled)
            predicted_class = np.argmax(prediction, axis=1)
            
            if predicted_class[0] == 1:
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                alert_message = f"Fall detected at {now}"
                print(alert_message)  # Print the fall detection and local time.
                send_alert(alert_message)  # Send the alert via Kafka

            buffer_coordenadas = []
    except ValueError as e:
        print(f"Error processing coordinates: {e}")
    except Exception as e:
        print(f"Error during prediction or sending alerts: {e}")
