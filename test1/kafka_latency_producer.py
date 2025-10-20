from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time
from datetime import datetime

def kafka_latency_producer():
    """
    Producer Kafka para teste de latência end-to-end.
    
    Marca o timestamp T0 no momento da criação da mensagem,
    antes da serialização e envio.
    """
    
    # === CRIAR TÓPICO SE NÃO EXISTIR ===
    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
    
    topic_name = 'latency_test'
    
    try:
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,  # 1 partição para simplificar
            replication_factor=1  # 1 réplica (single broker)
        )
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✓ Tópico '{topic_name}' criado!")
        time.sleep(2)  # Aguardar propagação do tópico
    except TopicAlreadyExistsError:
        print(f"✓ Tópico '{topic_name}' já existe.")
    except Exception as e:
        print(f"⚠️  Aviso ao criar tópico: {e}")
    
    # === CONECTAR AO KAFKA ===
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialização automática
        acks='all',  # Aguarda confirmação de todas as réplicas (mais lento, mais seguro)
        retries=3  # Tentar novamente em caso de falha
    )
    
    print("=" * 60)
    print("KAFKA - PRODUCER DE TESTE DE LATÊNCIA")
    print("=" * 60)
    print("Enviando 100 mensagens...\n")
    
    # === ENVIAR 100 MENSAGENS ===
    for i in range(100):
        # T0: Timestamp de criação da mensagem (ponto de partida)
        t0_creation = time.time() * 1000
        
        message = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'timestamp_ms': t0_creation,  # T0 armazenado na mensagem
            'producer': 'kafka'
        }
        
        # T1: Envio para o Kafka (serialização acontece automaticamente via value_serializer)
        t1_before_send = time.time() * 1000
        future = producer.send(topic_name, value=message)
        
        # Aguardar confirmação de cada mensagem (blocking)
        try:
            record_metadata = future.get(timeout=10)  # Espera até 10s por confirmação
            t1_after_send = time.time() * 1000
            send_time = t1_after_send - t1_before_send
            
            # Feedback a cada 10 mensagens
            if (i + 1) % 10 == 0:
                print(f"✓ Enviadas {i + 1}/100 mensagens | "
                      f"Partition: {record_metadata.partition} | "
                      f"Offset: {record_metadata.offset} | "
                      f"Tempo de envio: {send_time:.2f} ms")
        
        except Exception as e:
            print(f"❌ Erro ao enviar mensagem {i}: {e}")
    
    # Garantir que todas as mensagens foram enviadas
    producer.flush()
    
    print("\n✅ 100 mensagens enviadas com sucesso!")
    print("=" * 60)
    
    # Fechar conexão
    producer.close()

if __name__ == "__main__":
    kafka_latency_producer()
