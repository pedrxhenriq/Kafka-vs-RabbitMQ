from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import json
import time
import sys
from datetime import datetime

TOPIC_NAME = 'concurrency_test'

def create_topic(num_partitions=3):
    """Cria o tópico com número especificado de partições"""
    try:
        admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
        
        # Deletar tópico antigo se existir
        try:
            admin.delete_topics([TOPIC_NAME])
            print(f"Tópico '{TOPIC_NAME}' deletado (limpeza)")
            time.sleep(3)  # Aguardar exclusão
        except UnknownTopicOrPartitionError:
            pass
        
        # Criar novo tópico
        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=num_partitions,
            replication_factor=1
        )
        admin.create_topics([topic])
        print(f"Tópico '{TOPIC_NAME}' criado com {num_partitions} partições")
        time.sleep(2)
        admin.close()
    except TopicAlreadyExistsError:
        print(f"Tópico '{TOPIC_NAME}' já existe com partições existentes")
    except Exception as e:
        print(f"Aviso: {e}")

def producer(num_messages=1000, num_partitions=3):
    """Produz N mensagens sequenciais"""
    create_topic(num_partitions)
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )
    
    print("\n" + "=" * 70)
    print("TESTE DE CONCORRÊNCIA - KAFKA - PRODUCER")
    print("=" * 70)
    print(f"Tópico: {TOPIC_NAME}")
    print(f"Partições: {num_partitions}")
    print(f"\nEnviando {num_messages} mensagens...")
    
    start_time = time.time()
    
    for i in range(num_messages):
        message = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'data': f'Mensagem número {i}'
        }
        
        # Enviar sem key para distribuição round-robin entre partições
        future = producer.send(TOPIC_NAME, value=message)
        future.get(timeout=10)
        
        if (i + 1) % 100 == 0:
            print(f"  ✓ {i + 1} mensagens enviadas")
    
    producer.flush()
    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f"\n✓ Total: {num_messages} mensagens enviadas com sucesso!")
    print(f"  Tempo total: {elapsed:.2f} segundos")
    print(f"  Taxa: {num_messages/elapsed:.2f} msgs/segundo")
    
    print("\n" + "=" * 70)
    print("Agora execute 3 consumidores em terminais diferentes:")
    print("  Terminal 1: python kafka_concurrency_test.py consumer C1")
    print("  Terminal 2: python kafka_concurrency_test.py consumer C2")
    print("  Terminal 3: python kafka_concurrency_test.py consumer C3")
    print("=" * 70)
    
    producer.close()

def consumer(consumer_id):
    """Consome mensagens e registra detalhes"""
    group_id = 'concurrency_test_group'
    
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=30000  # 30 segundos de timeout
    )
    
    # Preparar arquivo de log
    log_filename = f'kafka_consumer_{consumer_id}.log'
    
    # Estado do consumidor
    state = {
        'consumer_id': consumer_id,
        'messages_received': [],
        'partitions_assigned': [],
        'timestamps': [],
        'start_time': None,
        'end_time': None
    }
    
    print("\n" + "=" * 70)
    print(f"CONSUMIDOR {consumer_id} - KAFKA - INICIADO")
    print("=" * 70)
    print(f"Tópico: {TOPIC_NAME}")
    print(f"Grupo: {group_id}")
    print(f"Logs sendo salvos em: {log_filename}")
    
    # Aguardar atribuição de partições
    print("\nAguardando atribuição de partições...")
    while not consumer.assignment():
        consumer.poll(timeout_ms=100)
        time.sleep(0.1)
    
    assigned_partitions = [tp.partition for tp in consumer.assignment()]
    state['partitions_assigned'] = assigned_partitions
    print(f"✓ Partições atribuídas: {assigned_partitions}")
    print("\nAguardando mensagens...\n")
    
    # Abrir arquivo de log
    log_file = open(log_filename, 'w', encoding='utf-8')
    log_file.write(f"=== Consumidor {consumer_id} - Kafka - Iniciado em {datetime.now().isoformat()} ===\n")
    log_file.write(f"Plataforma: Kafka\n")
    log_file.write(f"Tópico: {TOPIC_NAME}\n")
    log_file.write(f"Grupo: {group_id}\n")
    log_file.write(f"Partições atribuídas: {assigned_partitions}\n\n")
    
    try:
        for message in consumer:
            if state['start_time'] is None:
                state['start_time'] = time.time()
            
            receive_time = datetime.now()
            data = message.value
            msg_id = data['id']
            partition = message.partition
            
            state['messages_received'].append(msg_id)
            state['timestamps'].append(receive_time.isoformat())
            
            # Log detalhado
            log_line = f"[{receive_time.strftime('%H:%M:%S.%f')[:-3]}] Recebida mensagem ID={msg_id} (partition={partition}, offset={message.offset})\n"
            log_file.write(log_line)
            log_file.flush()
            
            # Print a cada 100 mensagens
            if len(state['messages_received']) % 100 == 0:
                print(f"  ✓ {len(state['messages_received'])} mensagens processadas")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Consumidor interrompido pelo usuário")
    except Exception as e:
        print(f"\n⚠️  Timeout ou fim das mensagens: {e}")
    finally:
        state['end_time'] = time.time()
        
        # Estatísticas finais
        total_messages = len(state['messages_received'])
        elapsed = state['end_time'] - state['start_time'] if state['start_time'] else 0
        rate = total_messages / elapsed if elapsed > 0 else 0
        
        print("\n" + "=" * 70)
        print(f"CONSUMIDOR {consumer_id} - ESTATÍSTICAS FINAIS")
        print("=" * 70)
        print(f"Partições atribuídas: {state['partitions_assigned']}")
        print(f"Total processado: {total_messages} mensagens")
        print(f"Tempo total: {elapsed:.2f} segundos")
        print(f"Taxa: {rate:.2f} msgs/segundo")
        print(f"IDs processados: {sorted(state['messages_received'][:10])}... (primeiros 10)")
        print("=" * 70)
        
        # Salvar resumo no log
        log_file.write(f"\n=== Consumidor {consumer_id} - Finalizado em {datetime.now().isoformat()} ===\n")
        log_file.write(f"Partições atribuídas: {state['partitions_assigned']}\n")
        log_file.write(f"Total processado: {total_messages} mensagens\n")
        log_file.write(f"Tempo total: {elapsed:.2f} segundos\n")
        log_file.write(f"Taxa: {rate:.2f} msgs/segundo\n")
        log_file.write(f"IDs processados (ordenados): {sorted(state['messages_received'])}\n")
        
        log_file.close()
        consumer.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python kafka_concurrency_test.py producer [num_messages] [num_partitions]")
        print("  python kafka_concurrency_test.py consumer <consumer_id>")
        print("\nExemplos:")
        print("  python kafka_concurrency_test.py producer 1000 1   # 1 partição")
        print("  python kafka_concurrency_test.py producer 1000 3   # 3 partições")
        print("  python kafka_concurrency_test.py producer 1000 6   # 6 partições")
        print("  python kafka_concurrency_test.py consumer C1")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'producer':
        num_msgs = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
        num_parts = int(sys.argv[3]) if len(sys.argv) > 3 else 3
        producer(num_msgs, num_parts)
    elif command == 'consumer':
        if len(sys.argv) < 3:
            print("Erro: Especifique o ID do consumidor (ex: C1, C2, C3)")
            sys.exit(1)
        consumer_id = sys.argv[2]
        consumer(consumer_id)
    else:
        print("Comando inválido! Use 'producer' ou 'consumer'")
