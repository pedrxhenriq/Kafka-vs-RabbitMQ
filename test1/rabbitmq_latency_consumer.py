import pika
import json
import time
from datetime import datetime
import statistics

# Estrutura para armazenar todos os timestamps detalhados
detailed_data = []
latencies = []

def callback(ch, method, properties, body):
    """
    Callback executado quando uma mensagem é recebida.
    
    Fluxo de timestamps:
    - T0: Criação da mensagem no producer (vem na mensagem)
    - T3: Recebimento da mensagem RAW (antes de processar)
    - T4: Após desserialização JSON
    """
    
    # T3: Timestamp de recebimento da mensagem RAW (antes de qualquer processamento)
    t3_receive_raw = time.time() * 1000
    
    # T4: Desserialização JSON (processamento da mensagem)
    t4_before_deserialize = time.time() * 1000
    message = json.loads(body)
    t4_after_deserialize = time.time() * 1000
    
    # T0: Recuperar timestamp de quando a mensagem foi criada no producer
    t0_creation = message['timestamp_ms']
    
    # Calcular latências
    end_to_end_latency = t3_receive_raw - t0_creation  # Latência total (sem desserialização)
    deserialization_time = t4_after_deserialize - t4_before_deserialize
    total_with_processing = t4_after_deserialize - t0_creation  # Com desserialização
    
    # Armazenar latência end-to-end (sem desserialização)
    latencies.append(end_to_end_latency)
    
    # Armazenar dados detalhados para análise posterior
    detailed_data.append({
        'message_id': message['id'],
        't0_creation_producer': t0_creation,
        't3_receive_raw_consumer': t3_receive_raw,
        't4_after_deserialize': t4_after_deserialize,
        'latency_end_to_end_ms': end_to_end_latency,
        'deserialization_time_ms': deserialization_time,
        'total_with_processing_ms': total_with_processing,
        'timestamp_iso': message['timestamp']
    })
    
    print(f"📨 Mensagem {message['id']:3d} | "
          f"Latência: {end_to_end_latency:6.2f} ms | "
          f"Desserialização: {deserialization_time:.3f} ms")
    
    # Confirmar processamento da mensagem (ACK)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # Parar após 100 mensagens
    if len(latencies) >= 100:
        ch.stop_consuming()

def rabbitmq_latency_consumer():
    """
    Consumer RabbitMQ para medir latência end-to-end.
    
    Mede o tempo desde a criação da mensagem no producer (T0)
    até o recebimento RAW no consumer (T3), SEM incluir desserialização.
    """
    
    # Conectar ao RabbitMQ
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    # Declarar fila (garante que existe)
    queue_name = 'latency_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    # Configurar QoS: processar 1 mensagem por vez
    channel.basic_qos(prefetch_count=1)
    
    # Registrar callback para processar mensagens
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    
    print("=" * 60)
    print("RABBITMQ - CONSUMER DE TESTE DE LATÊNCIA")
    print("=" * 60)
    print("🎧 Aguardando mensagens... Pressione CTRL+C para sair\n")
    
    # Iniciar consumo (bloqueia até stop_consuming)
    channel.start_consuming()
    
    # === CALCULAR ESTATÍSTICAS ===
    if latencies:
        print("\n" + "=" * 60)
        print("RESULTADOS RABBITMQ - LATÊNCIA END-TO-END")
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
        with open('rabbitmq_latency_results.txt', 'w', encoding='utf-8') as f:
            # Cabeçalho explicativo
            f.write("=" * 70 + "\n")
            f.write("RABBITMQ - RELATÓRIO DE LATÊNCIA END-TO-END\n")
            f.write("=" * 70 + "\n\n")
            
            f.write("📖 EXPLICAÇÃO DA MEDIÇÃO:\n")
            f.write("-" * 70 + "\n")
            f.write("Este teste mede a LATÊNCIA END-TO-END, que inclui:\n\n")
            f.write("  T0 → Criação da mensagem no PRODUCER\n")
            f.write("       - Marca o timestamp antes da serialização JSON\n")
            f.write("       - Armazenado no campo 'timestamp_ms' da mensagem\n\n")
            f.write("  T1 → Serialização JSON no producer\n")
            f.write("       - Conversão do dict Python para string JSON\n\n")
            f.write("  T2 → Envio pela rede + Processamento no RabbitMQ\n")
            f.write("       - Transmissão TCP/IP (localhost)\n")
            f.write("       - Persistência em disco (delivery_mode=2)\n")
            f.write("       - Enfileiramento no broker\n\n")
            f.write("  T3 → Recebimento RAW no CONSUMER\n")
            f.write("       - Timestamp capturado ANTES da desserialização\n")
            f.write("       - Este é o ponto final da medição\n\n")
            f.write("  T4 → Desserialização JSON no consumer\n")
            f.write("       - NÃO incluído na latência end-to-end\n")
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
                   f"{'T4_Deserial':>15} | {'Latency':>10} | {'Deserial':>10} | {'Total':>10}\n")
            f.write("-" * 70 + "\n")
            
            for data in detailed_data:
                f.write(f"{data['message_id']:4d} | "
                       f"{data['t0_creation_producer']:15.3f} | "
                       f"{data['t3_receive_raw_consumer']:15.3f} | "
                       f"{data['t4_after_deserialize']:15.3f} | "
                       f"{data['latency_end_to_end_ms']:10.3f} | "
                       f"{data['deserialization_time_ms']:10.3f} | "
                       f"{data['total_with_processing_ms']:10.3f}\n")
            
            f.write("\n" + "=" * 70 + "\n")
            f.write("LEGENDA:\n")
            f.write("  ID         = Identificador da mensagem\n")
            f.write("  T0_Creation = Timestamp de criação no producer (ms)\n")
            f.write("  T3_Receive  = Timestamp de recebimento RAW no consumer (ms)\n")
            f.write("  T4_Deserial = Timestamp após desserialização (ms)\n")
            f.write("  Latency     = Latência end-to-end SEM desserialização (ms)\n")
            f.write("  Deserial    = Tempo de desserialização JSON (ms)\n")
            f.write("  Total       = Tempo total COM desserialização (ms)\n")
            f.write("=" * 70 + "\n")
        
        print(f"\n💾 Resultados salvos em 'rabbitmq_latency_results.txt'")
    
    else:
        print("\n⚠️  Nenhuma mensagem foi recebida!")

if __name__ == "__main__":
    try:
        rabbitmq_latency_consumer()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrompido pelo usuário")
        if latencies:
            print(f"📊 {len(latencies)} mensagens processadas antes da interrupção")
