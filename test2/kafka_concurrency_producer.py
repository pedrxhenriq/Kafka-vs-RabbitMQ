from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time
from datetime import datetime

def kafka_concurrency_producer():
    """
    Producer Kafka para teste de concorrência.
    Envia 10.000 mensagens numeradas sequencialmente.
    """
    
    # Criar tópico se não existir
    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
    topic_name = 'concurrency_test'
    
    try:
        topic = NewTopic(
            name=topic_name,
            num_partitions=3,
            replication_factor=1
        )
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✓ Tópico '{topic_name}' criado com 3 partições!")
        time.sleep(2)
    except TopicAlreadyExistsError:
        print(f"✓ Tópico '{topic_name}' já existe.")
    except Exception as e:
        print(f"⚠️  Aviso: {e}")

    print("\n⏳ AGUARDE 15 SEGUNDOS para iniciar os 3 consumers...")
    print("   Abra 3 terminais e execute:")
    print("   python test2/kafka_concurrency_consumer.py 1")
    print("   python test2/kafka_concurrency_consumer.py 2")
    print("   python test2/kafka_concurrency_consumer.py 3\n")
    
    for i in range(15, 0, -1):
        print(f"   ⏱️  {i} segundos restantes...")
        time.sleep(1)
    print("✓ Iniciando envio de mensagens!\n")
        
    # Conectar producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )
    
    print(f"🔄 Carregando metadata do tópico '{topic_name}'...")
    partitions = producer.partitions_for(topic_name)

    if partitions is None:
        print(f"❌ Erro: Tópico '{topic_name}' não encontrado!")
        return

    print(f"✓ Metadata carregado! Partições disponíveis: {sorted(partitions)}")

    # VALIDAR NÚMERO DE PARTIÇÕES
    if len(partitions) != 3:
        print(f"\n⚠️  ERRO: O tópico tem {len(partitions)} partição(ões), mas esperava 3!")
        print(f"💡 Solução: Delete o tópico e execute novamente:")
        print(f"   kafka-topics --bootstrap-server localhost:9092 --delete --topic {topic_name}")
        producer.close()
        return

    print(f"✓ Tópico configurado corretamente com 3 partições!\n")
    time.sleep(1)
    
    print("=" * 70)
    print("KAFKA - PRODUCER DE TESTE DE CONCORRÊNCIA")
    print("=" * 70)
    print("Enviando 10.000 mensagens numeradas sequencialmente...\n")
    
    start_time = time.time()
    partition_tracker = {0: 0, 1: 0, 2: 0}
    
    # Enviar 10.000 mensagens
    futures_list = []

    # Enviar 10.000 mensagens
    for i in range(10000):
        timestamp_send = time.time() * 1000
        
        message = {
            'id': i,
            'sequence': i,
            'timestamp': datetime.now().isoformat(),
            'timestamp_ms': timestamp_send,
            'producer': 'kafka'
        }
        
        target_partition = i % 3
        
        # Enviar de forma ASSÍNCRONA (sem .get())
        future = producer.send(topic_name, value=message, partition=target_partition)
        futures_list.append((future, target_partition))  # Guardar para tracking
        
        # Feedback a cada 1000 mensagens
        if (i + 1) % 1000 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            # Contar partições estimadas (não esperar confirmação aqui)
            p0_count = ((i + 1) // 3) + (1 if (i + 1) % 3 >= 1 else 0)
            p1_count = ((i + 1) // 3) + (1 if (i + 1) % 3 >= 2 else 0)
            p2_count = (i + 1) // 3
            print(f"✓ Enviadas {i + 1}/10000 mensagens | "
                f"Taxa: {rate:.0f} msgs/s | "
                f"Distribuição estimada: P0={p0_count} P1={p1_count} P2={p2_count}")

    print("\n⏳ Aguardando confirmação de todas as mensagens...")
    producer.flush()

    # Contar distribuição real após confirmação
    partition_tracker = {0: 0, 1: 0, 2: 0}
    for future, expected_partition in futures_list:
        try:
            record_metadata = future.get(timeout=30)  # Timeout maior aqui
            partition_tracker[record_metadata.partition] += 1
        except Exception as e:
            print(f"⚠️  Erro ao confirmar mensagem: {e}")

    print("✓ Todas as mensagens confirmadas!")

    end_time = time.time()
    total_time = end_time - start_time
    final_rate = 10000 / total_time
    
    print("\n" + "=" * 70)
    print("✅ 10.000 mensagens enviadas com sucesso!")
    print(f"⏱️  Tempo total: {total_time:.2f} segundos")
    print(f"📊 Taxa média: {final_rate:.0f} mensagens/segundo")
    print(f"\n📊 DISTRIBUIÇÃO POR PARTIÇÃO:")
    print(f"   Partição 0: {partition_tracker[0]} mensagens")
    print(f"   Partição 1: {partition_tracker[1]} mensagens")
    print(f"   Partição 2: {partition_tracker[2]} mensagens")
    print("=" * 70)
    
    # Salvar resumo
    with open('kafka_concurrency_producer_summary.txt', 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("KAFKA - RESUMO DO PRODUCER (TESTE DE CONCORRÊNCIA)\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Total de mensagens enviadas: 10.000\n")
        f.write(f"Número de partições: 3\n")
        f.write(f"Distribuição:\n")
        f.write(f"  Partição 0: {partition_tracker[0]} mensagens\n")
        f.write(f"  Partição 1: {partition_tracker[1]} mensagens\n")
        f.write(f"  Partição 2: {partition_tracker[2]} mensagens\n")
        f.write(f"\nTempo total de envio: {total_time:.2f} segundos\n")
        f.write(f"Taxa média de envio: {final_rate:.0f} mensagens/segundo\n")
        f.write(f"Início: {datetime.fromtimestamp(start_time).isoformat()}\n")
        f.write(f"Fim: {datetime.fromtimestamp(end_time).isoformat()}\n")
        f.write("\n" + "=" * 70 + "\n")
    
    print("💾 Resumo salvo em 'kafka_concurrency_producer_summary.txt'\n")
    
    producer.close()

if __name__ == "__main__":
    kafka_concurrency_producer()