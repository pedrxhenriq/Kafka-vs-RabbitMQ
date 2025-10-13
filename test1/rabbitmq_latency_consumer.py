import pika
import json
import time
from datetime import datetime

latencies = []

def callback(ch, method, properties, body):
    # Receber mensagem
    receive_time = time.time() * 1000
    message = json.loads(body)
    
    # Calcular latência
    send_time = message['timestamp_ms']
    latency = receive_time - send_time
    latencies.append(latency)
    
    print(f"Mensagem {message['id']} - Latência: {latency:.2f} ms")
    
    # Confirmar processamento
    ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # Parar após 100 mensagens
    if len(latencies) >= 100:
        ch.stop_consuming()

def rabbitmq_latency_consumer():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    queue_name = 'latency_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    
    print("Aguardando mensagens... Pressione CTRL+C para sair")
    channel.start_consuming()
    
    # Calcular estatísticas
    if latencies:
        print("\n=== RESULTADOS RABBITMQ ===")
        print(f"Total de mensagens: {len(latencies)}")
        print(f"Latência média: {sum(latencies) / len(latencies):.2f} ms")
        print(f"Latência mínima: {min(latencies):.2f} ms")
        print(f"Latência máxima: {max(latencies):.2f} ms")
        
        # Salvar resultados em arquivo
        with open('rabbitmq_latency_results.txt', 'w') as f:
            f.write(f"Latência média: {sum(latencies) / len(latencies):.2f} ms\n")
            f.write(f"Latência mínima: {min(latencies):.2f} ms\n")
            f.write(f"Latência máxima: {max(latencies):.2f} ms\n")
            f.write(f"\nDetalhes:\n")
            for i, lat in enumerate(latencies):
                f.write(f"Mensagem {i}: {lat:.2f} ms\n")

if __name__ == "__main__":
    try:
        rabbitmq_latency_consumer()
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")