from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time
import sys

def create_topic():
    """Cria o tópico se não existir"""
    try:
        admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
        topic = NewTopic(name='delivery_test', num_partitions=1, replication_factor=1)
        admin.create_topics([topic])
        print("Tópico 'delivery_test' criado!")
        time.sleep(2)
        admin.close()
    except TopicAlreadyExistsError:
        print("Tópico 'delivery_test' já existe.")
    except Exception as e:
        print(f"Aviso: {e}")


def producer():
    """Envia 50 mensagens para o tópico"""
    create_topic()
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )
    
    topic_name = 'delivery_test'
    
    print("=" * 60)
    print("TESTE DE GARANTIA DE ENTREGA - KAFKA")
    print("=" * 60)
    print("\nEnviando 50 mensagens para o tópico...")
    
    for i in range(50):
        message = {
            'id': i,
            'data': f'Mensagem número {i}',
            'timestamp': time.time()
        }
        
        future = producer.send(topic_name, value=message)
        future.get(timeout=10)  # Aguardar confirmação
        
        if (i + 1) % 10 == 0:
            print(f"  ✓ {i + 1} mensagens enviadas")
    
    producer.flush()
    producer.close()
    
    print(f"\n✓ Total: 50 mensagens enviadas com sucesso!")
    print("\nAgora execute o consumidor:")
    print("  python kafka_delivery_test.py consumer")


def consumer_phase1():
    """Consome apenas 20 mensagens e simula interrupção"""
    topic_name = 'delivery_test'
    group_id = 'delivery_test_group'
    
    print("\n" + "=" * 60)
    print("FASE 1: CONSUMINDO MENSAGENS (será interrompido)")
    print("=" * 60)
    
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # IMPORTANTE: controle manual de offset
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    
    messages_received = []
    last_offset = None
    
    print("Iniciando consumo...")
    
    try:
        for message in consumer:
            data = message.value
            messages_received.append(data['id'])
            last_offset = message.offset
            
            print(f"  ✓ Recebida mensagem {data['id']} (offset: {message.offset})")
            time.sleep(0.1)
            
            # Interromper após 20 mensagens SEM fazer commit
            if len(messages_received) >= 20:
                print(f"\n⚠️  SIMULANDO INTERRUPÇÃO DO CONSUMIDOR!")
                print(f"    Mensagens lidas (mas NÃO commitadas): {len(messages_received)}")
                print(f"    IDs lidos: {messages_received}")
                print(f"    Último offset lido: {last_offset}")
                print(f"    ⚠️  Offset NÃO foi commitado (simulando falha)")
                break
    except Exception as e:
        print(f"Erro ou timeout: {e}")
    
    consumer.close()
    return messages_received


def consumer_phase2(first_batch_ids):
    """Reconecta e verifica o comportamento do Kafka"""
    print("\n" + "=" * 60)
    print("FASE 2: RECONECTANDO CONSUMIDOR")
    print("=" * 60)
    print("Aguardando 3 segundos antes de reconectar...")
    time.sleep(3)
    
    topic_name = 'delivery_test'
    group_id = 'delivery_test_group'
    
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,  # Agora com commit automático
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    
    messages_received = []
    
    print("Consumindo mensagens...")
    print("(Kafka irá reprocessar mensagens não commitadas)")
    
    try:
        for message in consumer:
            data = message.value
            messages_received.append(data['id'])
            
            print(f"  ✓ Recebida mensagem {data['id']} (offset: {message.offset})")
            time.sleep(0.1)
            
            # Processar até completar as 50
            if len(messages_received) >= 50:
                break
    except Exception as e:
        print(f"Timeout ou fim: {e}")
    
    consumer.close()
    
    # Análise dos resultados
    print("\n" + "=" * 60)
    print("RESULTADOS DO TESTE - KAFKA")
    print("=" * 60)
    print(f"\nFASE 1 (antes da interrupção - SEM commit):")
    print(f"  • Mensagens lidas: {len(first_batch_ids)}")
    print(f"  • IDs: {first_batch_ids}")
    
    print(f"\nFASE 2 (após reconexão - COM commit):")
    print(f"  • Mensagens processadas: {len(messages_received)}")
    print(f"  • IDs: {messages_received}")
    
    # Verificar reprocessamento
    all_ids = first_batch_ids + messages_received
    total_reads = len(all_ids)
    unique_ids = len(set(all_ids))
    
    print(f"\nTOTAL:")
    print(f"  • Total de leituras: {total_reads}")
    print(f"  • Mensagens únicas: {unique_ids}")
    
    if total_reads > unique_ids:
        reprocessed = total_reads - unique_ids
        print(f"  ⚠️  Mensagens REPROCESSADAS: {reprocessed}")
        
        # Encontrar quais foram reprocessadas
        duplicates = [id for id in all_ids if all_ids.count(id) > 1]
        print(f"  • IDs reprocessados: {sorted(set(duplicates))}")
    else:
        print(f"  ✓ Nenhuma mensagem foi reprocessada")
    
    # Verificar mensagens perdidas
    expected_ids = set(range(50))
    received_ids = set(all_ids)
    missing_ids = expected_ids - received_ids
    
    if missing_ids:
        print(f"  ⚠️  Mensagens perdidas: {sorted(missing_ids)}")
    else:
        print(f"  ✓ Todas as 50 mensagens foram entregues")
    
    print("\nCONCLUSÃO:")
    print("  Kafka REPROCESSA mensagens quando o offset não é commitado.")
    print("  Isso garante 'at-least-once delivery' (pelo menos uma entrega).")
    print("  O consumidor pode receber a mesma mensagem mais de uma vez.")
    print("=" * 60)
    
    # Salvar resultados
    with open('kafka_delivery_results.txt', 'w') as f:
        f.write("=== TESTE DE GARANTIA DE ENTREGA - KAFKA ===\n\n")
        f.write(f"Fase 1 (sem commit): {len(first_batch_ids)} mensagens\n")
        f.write(f"IDs Fase 1: {first_batch_ids}\n\n")
        f.write(f"Fase 2 (após reconexão): {len(messages_received)} mensagens\n")
        f.write(f"IDs Fase 2: {messages_received}\n\n")
        f.write(f"Total de leituras: {total_reads}\n")
        f.write(f"Mensagens únicas: {unique_ids}\n")
        f.write(f"Mensagens reprocessadas: {total_reads - unique_ids}\n")
        f.write(f"Mensagens perdidas: {len(missing_ids)}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python kafka_delivery_test.py producer   - Envia mensagens")
        print("  python kafka_delivery_test.py consumer   - Consome com interrupção")
        sys.exit(1)
    
    if sys.argv[1] == 'producer':
        producer()
    elif sys.argv[1] == 'consumer':
        first_batch = consumer_phase1()
        consumer_phase2(first_batch)
    else:
        print("Argumento inválido! Use 'producer' ou 'consumer'")