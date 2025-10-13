from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time
from datetime import datetime

def kafka_latency_producer():
    # Criar tópico se não existir
    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
    
    topic_name = 'latency_test'
    
    try:
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Tópico '{topic_name}' criado!")
        time.sleep(2)  # Aguardar criação
    except TopicAlreadyExistsError:
        print(f"Tópico '{topic_name}' já existe.")
    except Exception as e:
        print(f"Aviso ao criar tópico: {e}")
    
    # Conectar ao Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # Garantir que a mensagem foi escrita
        retries=3
    )
    
    print("Enviando 100 mensagens...")
    
    # Enviar 100 mensagens
    for i in range(100):
        message = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'timestamp_ms': time.time() * 1000
        }
        
        future = producer.send(topic_name, value=message)
        # Aguardar confirmação de cada mensagem
        try:
            record_metadata = future.get(timeout=10)
            if (i + 1) % 10 == 0:
                print(f"Enviadas {i + 1} mensagens (partition: {record_metadata.partition}, offset: {record_metadata.offset})")
        except Exception as e:
            print(f"Erro ao enviar mensagem {i}: {e}")
    
    producer.flush()
    print("100 mensagens enviadas com sucesso!")
    producer.close()

if __name__ == "__main__":
    kafka_latency_producer()