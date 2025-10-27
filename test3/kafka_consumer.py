from kafka import KafkaConsumer
import json
import time
from datetime import datetime

def kafka_throughput_consumer():
    """
    Consumer Kafka para teste de throughput.
    
    Consome 100.000 mensagens e mede a taxa de processamento.
    """
    
    topic_name = 'throughput_test'
    
    print("=" * 70)
    print("KAFKA - CONSUMER DE TESTE DE THROUGHPUT")
    print("=" * 70)
    print(f"🎧 Conectando ao tópico '{topic_name}'...\n")
    
    # === CONFIGURAR CONSUMER ===
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='throughput_test_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=30000,  # 30s timeout
        max_poll_records=500  # Processar até 500 mensagens por poll
    )
    
    print("✓ Consumer conectado!")
    print(f"📍 Partições: {consumer.assignment()}\n")
    print("🚀 Iniciando consumo...\n")
    
    messages_consumed = 0
    target_messages = 100_000
    
    # Timestamp inicial (T0)
    start_time = time.time()
    first_message_time = None
    
    # === CONSUMIR MENSAGENS ===
    try:
        for message in consumer:
            if first_message_time is None:
                first_message_time = time.time()
            
            messages_consumed += 1
            
            # Feedback a cada 10.000 mensagens
            if messages_consumed % 10_000 == 0:
                elapsed = time.time() - start_time
                current_rate = messages_consumed / (time.time() - first_message_time) if first_message_time else 0
                print(f"📥 Consumidas: {messages_consumed:,}/{target_messages:,} | "
                      f"Taxa: {current_rate:,.0f} msg/s | "
                      f"Tempo: {elapsed:.2f}s")
            
            # Parar após consumir todas as mensagens
            if messages_consumed >= target_messages:
                break
    
    except Exception as e:
        print(f"⚠️  Timeout ou erro: {e}")
    
    # Timestamp final (T1)
    end_time = time.time()
    
    consumer.close()
    
    # === CALCULAR MÉTRICAS ===
    if messages_consumed > 0 and first_message_time:
        actual_processing_time = end_time - first_message_time
        throughput = messages_consumed / actual_processing_time
        
        print("\n" + "=" * 70)
        print("RESULTADOS KAFKA - CONSUMER THROUGHPUT")
        print("=" * 70)
        print(f"✅ Mensagens consumidas: {messages_consumed:,}")
        print(f"⏱️  Tempo de processamento: {actual_processing_time:.2f}s")
        print(f"🚀 THROUGHPUT: {throughput:,.2f} mensagens/segundo")
        print(f"📊 Taxa média: {throughput/1000:.2f}k msg/s")
        print("=" * 70)
        
        # === SALVAR RESULTADOS ===
        with open('kafka_throughput_consumer_results.txt', 'w', encoding='utf-8') as f:
            f.write("=" * 70 + "\n")
            f.write("KAFKA - CONSUMER THROUGHPUT (TAXA DE CONSUMO)\n")
            f.write("=" * 70 + "\n\n")
            
            f.write("📊 RESULTADOS:\n")
            f.write("-" * 70 + "\n")
            f.write(f"Mensagens consumidas: {messages_consumed:,}\n")
            f.write(f"Tempo de processamento: {actual_processing_time:.3f}s\n")
            f.write(f"🚀 THROUGHPUT: {throughput:,.2f} msg/s\n")
            f.write(f"   Equivalente a: {throughput/1000:.2f}k msg/s\n")
            f.write(f"   Equivalente a: {throughput*60:,.0f} msg/minuto\n")
            f.write("=" * 70 + "\n")
        
        print(f"\n💾 Resultados salvos em 'kafka_throughput_consumer_results.txt'\n")
    
    else:
        print("\n⚠️  Nenhuma mensagem foi consumida!")

if __name__ == "__main__":
    kafka_throughput_consumer()