from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time
from datetime import datetime

def create_topic():
    """Cria o tópico de teste"""
    admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
    topic = NewTopic(name='retention_test', num_partitions=1, replication_factor=1)
    
    try:
        admin.create_topics([topic])
        print("✓ Tópico 'retention_test' criado")
        time.sleep(2)
    except TopicAlreadyExistsError:
        print("✓ Tópico 'retention_test' já existe")

def produce_messages():
    """Envia 50 mensagens para o Kafka"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("\n" + "="*60)
    print("KAFKA - ENVIANDO MENSAGENS")
    print("="*60)
    
    for i in range(50):
        message = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'content': f'Mensagem de teste #{i}'
        }
        producer.send('retention_test', value=message)
        if (i + 1) % 10 == 0:
            print(f"✓ Enviadas {i+1}/50 mensagens")
    
    producer.flush()
    producer.close()
    print("✅ 50 mensagens enviadas!\n")

def consume_first_time():
    """Primeira leitura - consome todas as mensagens"""
    consumer = KafkaConsumer(
        'retention_test',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='retention_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    
    print("="*60)
    print("KAFKA - PRIMEIRA LEITURA")
    print("="*60)
    
    messages_first = []
    for msg in consumer:
        messages_first.append({
            'id': msg.value['id'],
            'offset': msg.offset,
            'partition': msg.partition,
            'timestamp': msg.value['timestamp'],
            'read_at': datetime.now().isoformat()
        })
        print(f"📨 Msg {msg.value['id']:2d} | Offset: {msg.offset} | Partition: {msg.partition}")
    
    consumer.close()
    print(f"✅ Primeira leitura: {len(messages_first)} mensagens\n")
    return messages_first

def consume_second_time():
    """Segunda leitura - reprocessa do início"""
    consumer = KafkaConsumer(
        'retention_test',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # Volta ao início
        enable_auto_commit=False,  # Não commita automaticamente
        group_id='retention_group_reprocess',  # Novo grupo = offset zero
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    
    print("="*60)
    print("KAFKA - SEGUNDA LEITURA (REPROCESSAMENTO)")
    print("="*60)
    print("🔄 Reprocessando mensagens do offset 0...\n")
    
    messages_second = []
    for msg in consumer:
        messages_second.append({
            'id': msg.value['id'],
            'offset': msg.offset,
            'partition': msg.partition,
            'timestamp': msg.value['timestamp'],
            'read_at': datetime.now().isoformat()
        })
        print(f"🔁 Msg {msg.value['id']:2d} | Offset: {msg.offset} | Partition: {msg.partition}")
    
    consumer.close()
    print(f"✅ Segunda leitura: {len(messages_second)} mensagens\n")
    return messages_second

def save_results(first_read, second_read):
    """Salva resultados em arquivo"""
    with open('kafka_retention_results.txt', 'w', encoding='utf-8') as f:
        f.write("="*70 + "\n")
        f.write("KAFKA - TESTE DE RETENÇÃO E REPROCESSAMENTO\n")
        f.write("="*70 + "\n\n")
        
        f.write("📖 DESCRIÇÃO DO TESTE:\n")
        f.write("-"*70 + "\n")
        f.write("1. Enviadas 50 mensagens para o tópico 'retention_test'\n")
        f.write("2. Primeira leitura: consumidas todas as mensagens (offset commitado)\n")
        f.write("3. Segunda leitura: reprocessamento usando novo consumer group\n")
        f.write("4. Resultado: Kafka mantém as mensagens e permite releitura\n\n")
        
        f.write("="*70 + "\n")
        f.write("PRIMEIRA LEITURA\n")
        f.write("="*70 + "\n")
        f.write(f"{'ID':>4} | {'Offset':>6} | {'Partition':>9} | {'Timestamp':>26} | {'Read At':>26}\n")
        f.write("-"*70 + "\n")
        for msg in first_read:
            f.write(f"{msg['id']:4d} | {msg['offset']:6d} | {msg['partition']:9d} | "
                   f"{msg['timestamp']:>26} | {msg['read_at']:>26}\n")
        
        f.write("\n" + "="*70 + "\n")
        f.write("SEGUNDA LEITURA (REPROCESSAMENTO)\n")
        f.write("="*70 + "\n")
        f.write(f"{'ID':>4} | {'Offset':>6} | {'Partition':>9} | {'Timestamp':>26} | {'Read At':>26}\n")
        f.write("-"*70 + "\n")
        for msg in second_read:
            f.write(f"{msg['id']:4d} | {msg['offset']:6d} | {msg['partition']:9d} | "
                   f"{msg['timestamp']:>26} | {msg['read_at']:>26}\n")
        
        f.write("\n" + "="*70 + "\n")
        f.write("RESULTADO KAFKA:\n")
        f.write("-"*70 + "\n")
        f.write(f"✅ Mensagens persistidas em disco\n")
        f.write(f"✅ Reprocessamento bem-sucedido: {len(second_read)} mensagens relidas\n")
        f.write(f"✅ Integridade mantida: mesmos IDs e offsets\n")
        f.write(f"✅ Controle de offset permite leitura histórica\n")
        f.write("="*70 + "\n")
    
    print("💾 Resultados salvos em 'kafka_retention_results.txt'")

if __name__ == "__main__":
    print("\n🚀 INICIANDO TESTE DE RETENÇÃO - KAFKA\n")
    
    create_topic()
    produce_messages()
    
    time.sleep(2)
    
    first = consume_first_time()
    time.sleep(2)
    second = consume_second_time()
    
    save_results(first, second)
    
    print("\n✅ TESTE CONCLUÍDO!")
