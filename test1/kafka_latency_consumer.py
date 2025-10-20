from kafka import KafkaConsumer
import json
import time
import statistics

def kafka_latency_consumer():
    """
    Consumer Kafka para medir latência end-to-end.
    
    Fluxo de timestamps:
    - T0: Criação da mensagem no producer (vem na mensagem)
    - T3: Recebimento da mensagem RAW (antes de processar)
    - T4: Após desserialização JSON
    
    IMPORTANTE: value_deserializer=None para capturar T3 ANTES da desserialização!
    """
    
    topic_name = 'latency_test'
    
    print("=" * 60)
    print("KAFKA - CONSUMER DE TESTE DE LATÊNCIA")
    print("=" * 60)
    print(f"Conectando ao Kafka e inscrevendo no tópico '{topic_name}'...\n")
    
    # === CONFIGURAR CONSUMER ===
    # value_deserializer=None para desserializar manualmente
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # Ler desde o início do tópico
        enable_auto_commit=True,  # Commit automático de offsets
        group_id='latency_test_group',  # Grupo de consumo único
        value_deserializer=None,  # ← NÃO deserializar automaticamente
        consumer_timeout_ms=30000  # Timeout de 30s se não houver mensagens
    )
    
    # Estruturas para armazenar dados
    latencies = []
    detailed_data = []
    
    print("🎧 Aguardando mensagens...")
    print(f"📍 Partições atribuídas: {consumer.assignment()}\n")
    
    # === CONSUMIR MENSAGENS ===
    try:
        for message in consumer:
            # T3: Timestamp de recebimento da mensagem RAW (ANTES de qualquer processamento)
            t3_receive_raw = time.time() * 1000
            
            # T4: Desserialização manual (agora controlamos quando acontece)
            t4_before_deserialize = time.time() * 1000
            
            # Desserializar manualmente (message.value é bytes)
            data = json.loads(message.value.decode('utf-8'))
            
            t4_after_deserialize = time.time() * 1000
            
            # T0: Recuperar timestamp de quando a mensagem foi criada no producer
            t0_creation = data['timestamp_ms']
            
            # Calcular latências
            end_to_end_latency = t3_receive_raw - t0_creation  # Latência total (sem desserialização)
            deserialization_time = t4_after_deserialize - t4_before_deserialize
            total_with_processing = t4_after_deserialize - t0_creation  # Com desserialização
            
            # Armazenar latência end-to-end (sem desserialização)
            latencies.append(end_to_end_latency)
            
            # Armazenar dados detalhados para análise posterior
            detailed_data.append({
                'message_id': data['id'],
                't0_creation_producer': t0_creation,
                't3_receive_raw_consumer': t3_receive_raw,
                't4_after_deserialize': t4_after_deserialize,
                'latency_end_to_end_ms': end_to_end_latency,
                'deserialization_time_ms': deserialization_time,
                'total_with_processing_ms': total_with_processing,
                'partition': message.partition,
                'offset': message.offset,
                'timestamp_iso': data['timestamp']
            })
            
            print(f"📨 Mensagem {data['id']:3d} | "
                  f"Latência: {end_to_end_latency:6.2f} ms | "
                  f"Desserialização: {deserialization_time:.3f} ms | "
                  f"Partition: {message.partition} | Offset: {message.offset}")
            
            # Parar após 100 mensagens
            if len(latencies) >= 100:
                break
    
    except Exception as e:
        print(f"⚠️  Timeout ou erro: {e}")
    
    # Fechar consumer
    consumer.close()
    
    # === CALCULAR ESTATÍSTICAS ===
    if latencies:
        print("\n" + "=" * 60)
        print("RESULTADOS KAFKA - LATÊNCIA END-TO-END")
        print("=" * 60)
        print(f"📊 Total de mensagens: {len(latencies)}")
        print(f"⏱️  Latência média: {statistics.mean(latencies):.2f} ms")
        print(f"⚡ Latência mínima: {min(latencies):.2f} ms")
        print(f"🐌 Latência máxima: {max(latencies):.2f} ms")
        print(f"📈 Mediana: {statistics.median(latencies):.2f} ms")
        print(f"📊 Desvio padrão: {statistics.stdev(latencies):.2f} ms")
        
        # Calcular percentis
        sorted_latencies = sorted(latencies)
        p50 = sorted_latencies[int(len(sorted_latencies) * 0.50)]
        p90 = sorted_latencies[int(len(sorted_latencies) * 0.90)]
        p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
        p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)]
        
        print(f"\n📊 Percentis:")
        print(f"   P50: {p50:.2f} ms")
        print(f"   P90: {p90:.2f} ms")
        print(f"   P95: {p95:.2f} ms")
        print(f"   P99: {p99:.2f} ms")
        print("=" * 60)
        
        # === SALVAR RESULTADOS EM ARQUIVO ===
        with open('kafka_latency_results.txt', 'w', encoding='utf-8') as f:
            # Cabeçalho explicativo
            f.write("=" * 70 + "\n")
            f.write("KAFKA - RELATÓRIO DE LATÊNCIA END-TO-END\n")
            f.write("=" * 70 + "\n\n")
            
            f.write("📖 EXPLICAÇÃO DA MEDIÇÃO:\n")
            f.write("-" * 70 + "\n")
            f.write("Este teste mede a LATÊNCIA END-TO-END, que inclui:\n\n")
            f.write("  T0 → Criação da mensagem no PRODUCER\n")
            f.write("       - Marca o timestamp antes da serialização JSON\n")
            f.write("       - Armazenado no campo 'timestamp_ms' da mensagem\n\n")
            f.write("  T1 → Serialização JSON no producer\n")
            f.write("       - Conversão do dict Python para JSON bytes\n")
            f.write("       - Feita automaticamente pelo value_serializer\n\n")
            f.write("  T2 → Envio pela rede + Processamento no Kafka\n")
            f.write("       - Transmissão TCP/IP (localhost)\n")
            f.write("       - Persistência em disco (acks='all')\n")
            f.write("       - Replicação entre brokers (se houver réplicas)\n")
            f.write("       - Armazenamento em partição/offset\n\n")
            f.write("  T3 → Recebimento RAW no CONSUMER\n")
            f.write("       - Timestamp capturado ANTES da desserialização\n")
            f.write("       - Este é o ponto final da medição\n")
            f.write("       - IMPORTANTE: value_deserializer=None para controlar isso!\n\n")
            f.write("  T4 → Desserialização JSON no consumer\n")
            f.write("       - NÃO incluído na latência end-to-end\n")
            f.write("       - Desserialização manual com json.loads()\n")
            f.write("       - Medido separadamente para análise\n\n")
            f.write("LATÊNCIA END-TO-END = T3 - T0\n")
            f.write("(Tempo total desde criação até recebimento, sem processamento)\n")
            f.write("=" * 70 + "\n\n")
            
            # Resumo estatístico
            f.write("📊 RESUMO ESTATÍSTICO\n")
            f.write("-" * 70 + "\n")
            f.write(f"Total de mensagens: {len(latencies)}\n")
            f.write(f"Latência média: {statistics.mean(latencies):.2f} ms\n")
            f.write(f"Latência mínima: {min(latencies):.2f} ms\n")
            f.write(f"Latência máxima: {max(latencies):.2f} ms\n")
            f.write(f"Mediana: {statistics.median(latencies):.2f} ms\n")
            f.write(f"Desvio padrão: {statistics.stdev(latencies):.2f} ms\n")
            f.write(f"\nPercentis:\n")
            f.write(f"  P50: {p50:.2f} ms\n")
            f.write(f"  P90: {p90:.2f} ms\n")
            f.write(f"  P95: {p95:.2f} ms\n")
            f.write(f"  P99: {p99:.2f} ms\n")
            f.write("\n" + "=" * 70 + "\n\n")
            
            # Dados detalhados em formato tabular
            f.write("📋 DADOS DETALHADOS (FORMATO DATAFRAME)\n")
            f.write("-" * 70 + "\n")
            f.write(f"{'ID':>4} | {'T0_Creation':>15} | {'T3_Receive':>15} | "
                   f"{'T4_Deserial':>15} | {'Latency':>10} | {'Deserial':>10} | "
                   f"{'Total':>10} | {'Part':>4} | {'Offset':>6}\n")
            f.write("-" * 70 + "\n")
            
            for data in detailed_data:
                f.write(f"{data['message_id']:4d} | "
                       f"{data['t0_creation_producer']:15.3f} | "
                       f"{data['t3_receive_raw_consumer']:15.3f} | "
                       f"{data['t4_after_deserialize']:15.3f} | "
                       f"{data['latency_end_to_end_ms']:10.3f} | "
                       f"{data['deserialization_time_ms']:10.3f} | "
                       f"{data['total_with_processing_ms']:10.3f} | "
                       f"{data['partition']:4d} | "
                       f"{data['offset']:6d}\n")
            
            f.write("\n" + "=" * 70 + "\n")
            f.write("LEGENDA:\n")
            f.write("  ID         = Identificador da mensagem\n")
            f.write("  T0_Creation = Timestamp de criação no producer (ms)\n")
            f.write("  T3_Receive  = Timestamp de recebimento RAW no consumer (ms)\n")
            f.write("  T4_Deserial = Timestamp após desserialização (ms)\n")
            f.write("  Latency     = Latência end-to-end SEM desserialização (ms)\n")
            f.write("  Deserial    = Tempo de desserialização JSON (ms)\n")
            f.write("  Total       = Tempo total COM desserialização (ms)\n")
            f.write("  Part        = Partição do Kafka onde a mensagem foi armazenada\n")
            f.write("  Offset      = Offset da mensagem na partição\n")
            f.write("=" * 70 + "\n")
        
        print(f"\n💾 Resultados salvos em 'kafka_latency_results.txt'")
    
    else:
        print("\n⚠️  Nenhuma mensagem foi recebida!")
        print("Verifique se:")
        print("  1. O Kafka está rodando (localhost:9092)")
        print("  2. O producer foi executado antes")
        print("  3. O tópico 'latency_test' existe")

if __name__ == "__main__":
    kafka_latency_consumer()
