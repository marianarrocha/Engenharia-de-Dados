%pip install kafka-python

from kafka import KafkaConsumer
import json

def deserializer(message):
    return json.loads(message.decode('utf-8'))

consumer = KafkaConsumer(
    'meu-topico',  # Nome do tópico
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', 
    group_id='meu-grupo', 
    value_deserializer=deserializer 
)

for message in consumer:
    print(f"Mensagem recebida: {message.value}")

consumer.close()
