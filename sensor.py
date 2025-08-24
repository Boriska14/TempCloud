import time
import random
import paho.mqtt.client as mqtt
import json

broker = "localhost"
port = 1883
topic = "iot/sensor/temperature"

client = mqtt.Client()
client.connect(broker, port)

while True:
    temp = round(random.uniform(0, 55), 2)
    payload = json.dumps({"sensor_id": 1, "temperature": temp})
    client.publish(topic, payload)
    print(f"Data sent: {payload}")
    time.sleep(10)
