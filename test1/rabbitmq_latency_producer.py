import pika
import json
import time
from datetime import datetime

def rabbitmq_latency_producer():
    # Conectar ao RabbitMQ
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    # Declarar a fila
    queue_name = 'latency_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    print("Enviando 100 mensagens...")
    
    # Enviar 100 mensagens
    for i in range(100):
        message = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'timestamp_ms': time.time() * 1000  # timestamp em milissegundos
        }
        
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)  # mensagem persistente
        )
        
        if (i + 1) % 10 == 0:
            print(f"Enviadas {i + 1} mensagens")
    
    print("100 mensagens enviadas com sucesso!")
    connection.close()

if __name__ == "__main__":
    rabbitmq_latency_producer()