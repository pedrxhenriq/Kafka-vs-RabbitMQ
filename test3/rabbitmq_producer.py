import pika
import json
import time
from datetime import datetime

def rabbitmq_throughput_producer():
    """
    Producer RabbitMQ para teste de throughput.
    
    Envia 100.000 mensagens JSON e mede a taxa de processamento.
    """
    
    # Conectar ao RabbitMQ
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    # Declarar fila durável
    queue_name = 'throughput_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    print("=" * 70)
    print("RABBITMQ - TESTE DE THROUGHPUT (TAXA DE PROCESSAMENTO)")
    print("=" * 70)
    print("📊 Configuração:")
    print("   - Total de mensagens: 100.000")
    print("   - Tipo: JSON pequeno (~100 bytes)")
    print("   - Modo: Envio sequencial")
    print("   - Persistência: Habilitada (delivery_mode=2)")
    print("=" * 70)
    print("\n🚀 Iniciando envio...\n")
    
    total_messages = 100_000
    messages_sent = 0
    failed_messages = 0
    
    # Timestamp inicial (T0)
    start_time = time.time()
    
    # === ENVIAR 100.000 MENSAGENS ===
    for i in range(total_messages):
        message = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'timestamp_ms': time.time() * 1000,
            'data': f'Test message {i}',
            'payload': 'x' * 50  # Padding para ~100 bytes
        }
        
        try:
            message_body = json.dumps(message)
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2  # Persistente
                )
            )
            messages_sent += 1
            
            # Feedback a cada 10.000 mensagens
            if (i + 1) % 10_000 == 0:
                elapsed = time.time() - start_time
                current_rate = (i + 1) / elapsed
                print(f"📤 Progresso: {i + 1:,}/{total_messages:,} mensagens | "
                      f"Taxa atual: {current_rate:,.0f} msg/s | "
                      f"Tempo: {elapsed:.2f}s")
        
        except Exception as e:
            failed_messages += 1
            if failed_messages <= 10:
                print(f"❌ Erro ao enviar mensagem {i}: {e}")
    
    # Timestamp final (T1)
    end_time = time.time()
    
    # === CALCULAR MÉTRICAS ===
    total_time = end_time - start_time
    throughput = messages_sent / total_time
    
    print("\n" + "=" * 70)
    print("RESULTADOS RABBITMQ - THROUGHPUT")
    print("=" * 70)
    print(f"✅ Mensagens enviadas: {messages_sent:,}")
    print(f"❌ Mensagens com falha: {failed_messages:,}")
    print(f"⏱️  Tempo total: {total_time:.2f} segundos")
    print(f"🚀 THROUGHPUT: {throughput:,.2f} mensagens/segundo")
    print(f"📊 Taxa média: {throughput/1000:.2f}k msg/s")
    print("=" * 70)
    
    # === SALVAR RESULTADOS ===
    with open('rabbitmq_throughput_results.txt', 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("RABBITMQ - RELATÓRIO DE THROUGHPUT (TAXA DE PROCESSAMENTO)\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("📖 METODOLOGIA DO TESTE:\n")
        f.write("-" * 70 + "\n")
        f.write("Este teste avalia a capacidade de processamento contínuo do RabbitMQ\n")
        f.write("sob alto volume de mensagens, medindo o throughput global do sistema.\n\n")
        f.write("CONFIGURAÇÃO:\n")
        f.write(f"  - Total de mensagens: {total_messages:,}\n")
        f.write("  - Tipo de mensagem: JSON (~100 bytes)\n")
        f.write("  - Modo de envio: Sequencial (produtor único)\n")
        f.write("  - Persistência: Habilitada (delivery_mode=2)\n")
        f.write("  - Broker: localhost:5672\n")
        f.write("  - Fila: throughput_test (durável)\n\n")
        f.write("MÉTRICA PRINCIPAL:\n")
        f.write("  Throughput = Total de mensagens / Tempo total\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("📊 RESULTADOS:\n")
        f.write("-" * 70 + "\n")
        f.write(f"Mensagens enviadas: {messages_sent:,}\n")
        f.write(f"Mensagens com falha: {failed_messages:,}\n")
        f.write(f"Tempo total: {total_time:.3f}s\n")
        f.write(f"Taxa de sucesso: {(messages_sent/total_messages)*100:.2f}%\n\n")
        f.write(f"🚀 THROUGHPUT: {throughput:,.2f} msg/s\n")
        f.write(f"   Equivalente a: {throughput/1000:.2f}k msg/s\n")
        f.write(f"   Equivalente a: {throughput*60:,.0f} msg/minuto\n")
        f.write(f"   Equivalente a: {throughput*3600:,.0f} msg/hora\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("📈 ANÁLISE:\n")
        f.write("-" * 70 + "\n")
        f.write(f"Início: {datetime.fromtimestamp(start_time).isoformat()}\n")
        f.write(f"Fim: {datetime.fromtimestamp(end_time).isoformat()}\n")
        f.write(f"Tempo médio/msg: {(total_time/messages_sent)*1000:.3f} ms\n")
        f.write("=" * 70 + "\n")
    
    print(f"\n💾 Resultados salvos em 'rabbitmq_throughput_results.txt'\n")
    
    connection.close()

if __name__ == "__main__":
    rabbitmq_throughput_producer()