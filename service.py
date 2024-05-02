import os
import numpy as np
import json
import datetime
from tensorflow.keras.models import load_model
from joblib import load
from kafka import KafkaConsumer
import http.client

n_pasos = 600
ventana_deslizante = 50  # Tamaño del deslizamiento de la ventana

# Configuración de Kafka y HTTP Bridge
kafka_bridge_password = os.getenv('KAFKA_BRDIGE_PASSWORD')
kafka_password = os.getenv('KAFKA_PASSWORD')
KAFKA_HTTP_BRIDGE = 'api.kafka.safewchair.duckdns.org'
ALERTS_TOPIC = 'Alerts'
headers = {
    'content-type': 'application/vnd.kafka.json.v2+json',
    'Authorization': f'Basic {kafka_bridge_password}'
}
KAFKA_BROKERS = 'kafka.safewchair.duckdns.org:31565'
CONSUMER_TOPIC = 'Testing'
SECURITY_PROTOCOL = 'SASL_PLAINTEXT'
SASL_MECHANISM = 'SCRAM-SHA-512'
USERNAME = 'safewchair'
PASSWORD = kafka_password

# Carga de modelos y escaladores
model_acc = load_model('model_acc.h5')
model_gyro = load_model('model_gyro.h5')
scaler_acc = load('scaler_acc.joblib')
scaler_gyro = load('scaler_gyro.joblib')

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

buffer_acc = []
buffer_gyro = []
cooldown_time = None

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
        cooldown_time = now + datetime.timedelta(seconds=15)
        print(f"Alert sent. Cooldown set until {cooldown_time}.")

for message in consumer:
    coordenadas_str = message.value
    try:
        coordenadas = np.fromstring(coordenadas_str, dtype=float, sep=',')
        buffer_acc.append(coordenadas[:3])
        buffer_gyro.append(coordenadas[3:])

        if len(buffer_acc) >= n_pasos:
            # Procesar la ventana actual
            data_acc = np.array(buffer_acc[:n_pasos]).flatten().reshape(1, -1)
            data_gyro = np.array(buffer_gyro[:n_pasos]).flatten().reshape(1, -1)
            
            # Escalar y reestructurar
            data_acc_scaled = scaler_acc.transform(data_acc)
            data_gyro_scaled = scaler_gyro.transform(data_gyro)
            data_acc_scaled = data_acc_scaled.reshape((1, n_pasos, 3))
            data_gyro_scaled = data_gyro_scaled.reshape((1, n_pasos, 3))
            
            # Predicción
            pred_acc = model_acc.predict(data_acc_scaled)
            pred_gyro = model_gyro.predict(data_gyro_scaled)
            fall_detected_acc = np.argmax(pred_acc, axis=1)[0] == 1
            fall_detected_gyro = np.argmax(pred_gyro, axis=1)[0] == 1

            # Si ambos modelos detectan una caída
            if fall_detected_acc and fall_detected_gyro:
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                alert_message = f"Fall detected at {now}"
                print(alert_message)
                send_alert(alert_message)

            # Mover la ventana deslizante
            buffer_acc = buffer_acc[ventana_deslizante:]
            buffer_gyro = buffer_gyro[ventana_deslizante:]
    except ValueError as e:
        print(f"Error processing coordinates: {e}")
    except Exception as e:
        print(f"Error during prediction or sending alerts: {e}")
