from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time
from datetime import datetime

def kafka_throughput_producer():
    """
    Producer Kafka para teste de throughput (taxa de processamento).
    
    Envia 100.000 mensagens JSON sequencialmente e mede:
    - Tempo total de envio
    - Taxa de processamento (mensagens/segundo)
    """
    
    # === CRIAR TÓPICO SE NÃO EXISTIR ===
    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
    topic_name = 'throughput_test'
    
    try:
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=1
        )
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✓ Tópico '{topic_name}' criado!")
        time.sleep(2)
    except TopicAlreadyExistsError:
        print(f"✓ Tópico '{topic_name}' já existe.")
    except Exception as e:
        print(f"⚠️  Aviso ao criar tópico: {e}")
    
    # === CONECTAR AO KAFKA ===
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # Aguarda todas as réplicas
        retries=3,
        linger_ms=10,  # Aguarda 10ms para fazer batch
        batch_size=16384  # Tamanho do batch em bytes
    )
    
    print("=" * 70)
    print("KAFKA - TESTE DE THROUGHPUT (TAXA DE PROCESSAMENTO)")
    print("=" * 70)
    print("📊 Configuração:")
    print("   - Total de mensagens: 100.000")
    print("   - Tipo: JSON pequeno (~100 bytes)")
    print("   - Modo: Envio sequencial")
    print("   - ACK: all (máxima confiabilidade)")
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
            # Envio assíncrono (não aguarda confirmação individual)
            future = producer.send(topic_name, value=message)
            messages_sent += 1
            
            # Feedback a cada 10.000 mensagens
            if (i + 1) % 10_000 == 0:
                elapsed = time.time() - start_time
                current_rate = (i + 1) / elapsed
                print(f"📤 Progresso: {i + 1:,}/{total_messages:,} mensagens | "
                      f"Taxa atual: {current_rate:,.0f} msg/s | "
                      f"Tempo decorrido: {elapsed:.2f}s")
        
        except Exception as e:
            failed_messages += 1
            if failed_messages <= 10:  # Mostrar apenas as primeiras falhas
                print(f"❌ Erro ao enviar mensagem {i}: {e}")
    
    # Aguardar todas as mensagens serem confirmadas
    print("\n⏳ Aguardando confirmação de todas as mensagens...")
    producer.flush()
    
    # Timestamp final (T1)
    end_time = time.time()
    
    # === CALCULAR MÉTRICAS ===
    total_time = end_time - start_time
    throughput = messages_sent / total_time
    
    print("\n" + "=" * 70)
    print("RESULTADOS KAFKA - THROUGHPUT")
    print("=" * 70)
    print(f"✅ Mensagens enviadas com sucesso: {messages_sent:,}")
    print(f"❌ Mensagens com falha: {failed_messages:,}")
    print(f"⏱️  Tempo total: {total_time:.2f} segundos")
    print(f"🚀 THROUGHPUT: {throughput:,.2f} mensagens/segundo")
    print(f"📊 Taxa média: {throughput/1000:.2f}k msg/s")
    print("=" * 70)
    
    # === SALVAR RESULTADOS ===
    with open('kafka_throughput_results.txt', 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("KAFKA - RELATÓRIO DE THROUGHPUT (TAXA DE PROCESSAMENTO)\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("📖 METODOLOGIA DO TESTE:\n")
        f.write("-" * 70 + "\n")
        f.write("Este teste avalia a capacidade de processamento contínuo do Kafka\n")
        f.write("sob alto volume de mensagens, medindo o throughput global do sistema.\n\n")
        f.write("CONFIGURAÇÃO:\n")
        f.write(f"  - Total de mensagens: {total_messages:,}\n")
        f.write("  - Tipo de mensagem: JSON (~100 bytes)\n")
        f.write("  - Modo de envio: Sequencial (produtor único)\n")
        f.write("  - ACK mode: all (aguarda réplicas)\n")
        f.write("  - Broker: localhost:9092\n")
        f.write("  - Tópico: throughput_test (1 partição, 1 réplica)\n\n")
        f.write("MÉTRICA PRINCIPAL:\n")
        f.write("  Throughput = Total de mensagens / Tempo total\n")
        f.write("  (Mensagens processadas por segundo)\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("📊 RESULTADOS:\n")
        f.write("-" * 70 + "\n")
        f.write(f"Total de mensagens enviadas: {messages_sent:,}\n")
        f.write(f"Mensagens com falha: {failed_messages:,}\n")
        f.write(f"Tempo total de execução: {total_time:.3f} segundos\n")
        f.write(f"Taxa de sucesso: {(messages_sent/total_messages)*100:.2f}%\n\n")
        f.write(f"🚀 THROUGHPUT: {throughput:,.2f} mensagens/segundo\n")
        f.write(f"   Equivalente a: {throughput/1000:.2f}k msg/s\n")
        f.write(f"   Equivalente a: {throughput*60:,.0f} msg/minuto\n")
        f.write(f"   Equivalente a: {throughput*3600:,.0f} msg/hora\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("📈 ANÁLISE DE DESEMPENHO:\n")
        f.write("-" * 70 + "\n")
        f.write(f"Timestamp início: {datetime.fromtimestamp(start_time).isoformat()}\n")
        f.write(f"Timestamp fim: {datetime.fromtimestamp(end_time).isoformat()}\n")
        f.write(f"Duração: {total_time:.3f}s\n")
        f.write(f"Tempo médio por mensagem: {(total_time/messages_sent)*1000:.3f} ms\n")
        f.write("=" * 70 + "\n")
    
    print(f"\n💾 Resultados salvos em 'kafka_throughput_results.txt'\n")
    
    producer.close()

if __name__ == "__main__":
    kafka_throughput_producer()