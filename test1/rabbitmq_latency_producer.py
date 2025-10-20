import pika
import json
import time
from datetime import datetime

def rabbitmq_latency_producer():
    """
    Producer RabbitMQ para teste de latência end-to-end.
    
    Marca o timestamp T0 no momento da criação da mensagem,
    antes da serialização e envio.
    """
    
    # Estabelecer conexão com RabbitMQ
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    # Declarar fila durável (persiste reinicializações)
    queue_name = 'latency_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    print("=" * 60)
    print("RABBITMQ - PRODUCER DE TESTE DE LATÊNCIA")
    print("=" * 60)
    print("Enviando 100 mensagens...")
    
    # Enviar 100 mensagens com timestamp
    for i in range(100):
        # T0: Timestamp de criação da mensagem (ponto de partida)
        t0_creation = time.time() * 1000
        
        message = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'timestamp_ms': t0_creation,  # T0 armazenado na mensagem
            'producer': 'rabbitmq'
        }
        
        # T1: Serialização JSON (início do envio)
        t1_before_serialize = time.time() * 1000
        message_body = json.dumps(message)
        t1_after_serialize = time.time() * 1000
        
        # T2: Envio para o broker (publish)
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2  # Mensagem persistente (escrita em disco)
            )
        )
        
        # Feedback a cada 10 mensagens
        if (i + 1) % 10 == 0:
            serialization_time = t1_after_serialize - t1_before_serialize
            print(f"✓ Enviadas {i + 1}/100 mensagens "
                  f"(serialização: {serialization_time:.3f} ms)")
    
    print("\n✅ 100 mensagens enviadas com sucesso!")
    print("=" * 60)
    
    # Fechar conexão
    connection.close()

if __name__ == "__main__":
    rabbitmq_latency_producer()
