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

# Configuración del Kafka HTTP Bridge
KAFKA_HTTP_BRIDGE = 'api.kafka.safewchair.duckdns.org'
ALERTS_TOPIC = 'Alerts'
headers = {
    'content-type': 'application/vnd.kafka.json.v2+json',
    'Authorization': f'Basic {kafka_bridge_password}'
}

# Configuración del consumer de Kafka
KAFKA_BROKERS = 'kafka.safewchair.duckdns.org:31565'
CONSUMER_TOPIC = 'Testing'
SECURITY_PROTOCOL = 'SASL_PLAINTEXT'
SASL_MECHANISM = 'SCRAM-SHA-512'
USERNAME = 'safewchair'
PASSWORD = kafka_password

# Cargar el modelo y el scaler
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

def send_alert(message):
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
    #print(f"Status: {response.status}, Response: {response.read().decode('utf-8')}")
    conn.close()

for message in consumer:
    # print(f"Mensaje recibido: {message.value}")
    coordenadas_str = message.value  # Asumiendo que cada mensaje es una cadena de coordenadas separadas por comas
    try:
        coordenadas = np.fromstring(coordenadas_str, dtype=float, sep=',')
        buffer_coordenadas.append(coordenadas)
        
        if len(buffer_coordenadas) == 300:
            data = np.array(buffer_coordenadas)
            data_flattened = data.flatten().reshape(1, -1)
            data_scaled = scaler.transform(data_flattened)
            data_scaled = data_scaled.reshape((1, 300, 6))
            
            prediction = model.predict(data_scaled)
            predicted_class = np.argmax(prediction, axis=1)
            
            if predicted_class[0] == 1:
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                alert_message = f"Caída detectada en {now}"
                print(alert_message)  # Imprime la detección de caída y la hora localmente.
                send_alert(alert_message)  # Envía la alerta a través de Kafka

            buffer_coordenadas = []
    except ValueError as e:
        print(f"Error al procesar las coordenadas: {e}")
