import pika
import json
import time
from datetime import datetime

def rabbitmq_concurrency_producer():
    """
    Producer RabbitMQ para teste de concorrência.
    Envia 10.000 mensagens numeradas sequencialmente.
    """
    
    # Conectar ao RabbitMQ
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    # Declarar fila durável
    queue_name = 'concurrency_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    print("=" * 70)
    print("RABBITMQ - PRODUCER DE TESTE DE CONCORRÊNCIA")
    print("=" * 70)
    print("Enviando 10.000 mensagens numeradas sequencialmente...\n")
    
    start_time = time.time()
    
    # Enviar 10.000 mensagens
    for i in range(10000):
        timestamp_send = time.time() * 1000
        
        message = {
            'id': i,
            'sequence': i,  # Número sequencial explícito
            'timestamp': datetime.now().isoformat(),
            'timestamp_ms': timestamp_send,
            'producer': 'rabbitmq'
        }
        
        message_body = json.dumps(message)
        
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2  # Persistente
            )
        )
        
        # Feedback a cada 1000 mensagens
        if (i + 1) % 1000 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            print(f"✓ Enviadas {i + 1}/10000 mensagens | "
                  f"Taxa: {rate:.0f} msgs/s | "
                  f"Tempo: {elapsed:.2f}s")
    
    end_time = time.time()
    total_time = end_time - start_time
    final_rate = 10000 / total_time
    
    print("\n" + "=" * 70)
    print("✅ 10.000 mensagens enviadas com sucesso!")
    print(f"⏱️  Tempo total: {total_time:.2f} segundos")
    print(f"📊 Taxa média: {final_rate:.0f} mensagens/segundo")
    print("=" * 70)
    
    # Salvar resumo do producer
    with open('rabbitmq_concurrency_producer_summary.txt', 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("RABBITMQ - RESUMO DO PRODUCER (TESTE DE CONCORRÊNCIA)\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Total de mensagens enviadas: 10.000\n")
        f.write(f"Tempo total de envio: {total_time:.2f} segundos\n")
        f.write(f"Taxa média de envio: {final_rate:.0f} mensagens/segundo\n")
        f.write(f"Início: {datetime.fromtimestamp(start_time).isoformat()}\n")
        f.write(f"Fim: {datetime.fromtimestamp(end_time).isoformat()}\n")
        f.write("\n" + "=" * 70 + "\n")
    
    print("💾 Resumo salvo em 'rabbitmq_concurrency_producer_summary.txt'\n")
    
    connection.close()

if __name__ == "__main__":
    rabbitmq_concurrency_producer()