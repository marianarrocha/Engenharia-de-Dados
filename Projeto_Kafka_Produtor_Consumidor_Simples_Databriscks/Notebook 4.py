%pip install kafka-python

from kafka import KafkaProducer
import time
import json

def serializer(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer = serializer
    )

for i in range(10):
    message = {'numero': i, 'mensagem': f"Mensagem {i}"}
    producer.send('meu-topico', value=message)
    print(f"Mensagem enviada: {message}")
    time.sleep(1)

producer.close()
