import pika
import json
import time
from datetime import datetime

# Controle global
messages_consumed = 0
start_time = None
first_message_time = None
target_messages = 100_000

def callback(ch, method, properties, body):
    """Callback para processar mensagens."""
    global messages_consumed, first_message_time
    
    if first_message_time is None:
        first_message_time = time.time()
    
    messages_consumed += 1
    
    # Feedback a cada 10.000 mensagens
    if messages_consumed % 10_000 == 0:
        elapsed = time.time() - start_time
        current_rate = messages_consumed / (time.time() - first_message_time)
        print(f"📥 Consumidas: {messages_consumed:,}/{target_messages:,} | "
              f"Taxa: {current_rate:,.0f} msg/s | "
              f"Tempo: {elapsed:.2f}s")
    
    # ACK manual
    ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # Parar após consumir todas
    if messages_consumed >= target_messages:
        ch.stop_consuming()

def rabbitmq_throughput_consumer():
    """Consumer RabbitMQ para teste de throughput."""
    global start_time
    
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    queue_name = 'throughput_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    # QoS: prefetch de 100 mensagens
    channel.basic_qos(prefetch_count=100)
    
    print("=" * 70)
    print("RABBITMQ - CONSUMER DE TESTE DE THROUGHPUT")
    print("=" * 70)
    print(f"🎧 Conectado à fila '{queue_name}'")
    print("🚀 Iniciando consumo...\n")
    
    start_time = time.time()
    
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    channel.start_consuming()
    
    # Calcular resultados
    end_time = time.time()
    
    if messages_consumed > 0 and first_message_time:
        processing_time = end_time - first_message_time
        throughput = messages_consumed / processing_time
        
        print("\n" + "=" * 70)
        print("RESULTADOS RABBITMQ - CONSUMER THROUGHPUT")
        print("=" * 70)
        print(f"✅ Mensagens consumidas: {messages_consumed:,}")
        print(f"⏱️  Tempo de processamento: {processing_time:.2f}s")
        print(f"🚀 THROUGHPUT: {throughput:,.2f} msg/s")
        print(f"📊 Taxa média: {throughput/1000:.2f}k msg/s")
        print("=" * 70)
        
        with open('rabbitmq_throughput_consumer_results.txt', 'w', encoding='utf-8') as f:
            f.write("=" * 70 + "\n")
            f.write("RABBITMQ - CONSUMER THROUGHPUT\n")
            f.write("=" * 70 + "\n\n")
            f.write(f"Mensagens consumidas: {messages_consumed:,}\n")
            f.write(f"Tempo: {processing_time:.3f}s\n")
            f.write(f"🚀 THROUGHPUT: {throughput:,.2f} msg/s\n")
            f.write(f"   Equivalente: {throughput/1000:.2f}k msg/s\n")
            f.write("=" * 70 + "\n")
        
        print(f"\n💾 Resultados salvos em 'rabbitmq_throughput_consumer_results.txt'\n")
    
    connection.close()

if __name__ == "__main__":
    try:
        rabbitmq_throughput_consumer()
    except KeyboardInterrupt:
        print(f"\n⚠️  Interrompido! Mensagens consumidas: {messages_consumed:,}")