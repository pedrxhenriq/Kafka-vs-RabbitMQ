from kafka import KafkaConsumer
import json
import time

def kafka_latency_consumer():
    topic_name = 'latency_test'
    
    print(f"Conectando ao Kafka e inscrevendo no tópico '{topic_name}'...")
    
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # Ler desde o início
        enable_auto_commit=True,
        group_id='latency_test_group',  # Importante: grupo único
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=30000  # Timeout de 30 segundos se não houver mensagens
    )
    
    latencies = []
    
    print("Aguardando mensagens...")
    print(f"Partições atribuídas: {consumer.assignment()}")
    
    try:
        for message in consumer:
            receive_time = time.time() * 1000
            data = message.value
            
            send_time = data['timestamp_ms']
            latency = receive_time - send_time
            latencies.append(latency)
            
            print(f"Mensagem {data['id']} - Latência: {latency:.2f} ms (partition: {message.partition}, offset: {message.offset})")
            
            # Parar após 100 mensagens
            if len(latencies) >= 100:
                break
    except Exception as e:
        print(f"Timeout ou erro: {e}")
    
    consumer.close()
    
    # Calcular estatísticas
    if latencies:
        print("\n=== RESULTADOS KAFKA ===")
        print(f"Total de mensagens: {len(latencies)}")
        print(f"Latência média: {sum(latencies) / len(latencies):.2f} ms")
        print(f"Latência mínima: {min(latencies):.2f} ms")
        print(f"Latência máxima: {max(latencies):.2f} ms")
        
        # Salvar resultados
        with open('kafka_latency_results.txt', 'w') as f:
            f.write(f"Latência média: {sum(latencies) / len(latencies):.2f} ms\n")
            f.write(f"Latência mínima: {min(latencies):.2f} ms\n")
            f.write(f"Latência máxima: {max(latencies):.2f} ms\n")
            f.write(f"\nDetalhes:\n")
            for i, lat in enumerate(latencies):
                f.write(f"Mensagem {i}: {lat:.2f} ms\n")
        print("\nResultados salvos em 'kafka_latency_results.txt'")
    else:
        print("\nNenhuma mensagem foi recebida!")

if __name__ == "__main__":
    kafka_latency_consumer()