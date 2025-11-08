# kafka_progressive_threshold.py

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time
import threading
from datetime import datetime
import statistics

# Configuração - AJUSTADA PARA SER MAIS AGRESSIVA
TEST_ID = int(time.time())
INITIAL_BATCH = 50000  # Começa com 50k mensagens
BATCH_INCREMENT = 50000  # Aumenta 50k por fase
MAX_PHASES = 20
PHASE_DURATION = 5  # Reduzido para 5s

# Métricas
phase_metrics = []
messages_sent = 0
messages_received = 0
consumer_running = True
start_time = None

class PhaseMetrics:
    def __init__(self, phase_id, target_messages):
        self.phase_id = phase_id
        self.target_messages = target_messages
        self.sent_count = 0
        self.received_count = 0
        self.start_time = 0
        self.end_time = 0
        self.latencies = []
        self.actual_send_rate = 0
        self.actual_receive_rate = 0
        self.avg_latency = 0
        self.max_latency = 0
        self.p99_latency = 0
        self.lost_messages = 0
        self.degradation_percent = 0

def delete_and_create_topic():
    try:
        admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
        try:
            admin.delete_topics(['progressive_test'])
            print("🗑️  Tópico deletado")
            time.sleep(5)
        except:
            pass
        
        topic = NewTopic(name='progressive_test', num_partitions=3, replication_factor=1)
        admin.create_topics([topic])
        print("✅ Tópico criado\n")
        time.sleep(3)
    except Exception as e:
        print(f"⚠️  {e}\n")

def consumer_thread():
    global messages_received, consumer_running
    
    try:
        consumer = KafkaConsumer(
            'progressive_test',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f'progressive_group_{TEST_ID}',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=30000,
            max_poll_records=1000
        )
        
        print("🎧 Consumer iniciado\n")
        
        for msg in consumer:
            t_receive = time.time() * 1000
            t_sent = msg.value['timestamp_ms']
            phase_id = msg.value['phase_id']
            latency = t_receive - t_sent
            
            if phase_id < len(phase_metrics):
                phase_metrics[phase_id].latencies.append(latency)
                phase_metrics[phase_id].received_count += 1
            
            messages_received += 1
    
    except Exception as e:
        print(f"⚠️  Consumer finalizado: {e}")
    finally:
        consumer_running = False
        try:
            consumer.close()
        except:
            pass

def detect_threshold_progressive(metrics):
    """
    Detecta threshold com critérios mais sensíveis:
    - Latência aumenta >100% em relação à baseline
    - Perda de mensagens >10%
    - Taxa de envio cai >30% entre fases consecutivas
    """
    if len(metrics) < 3:
        return None
    
    baseline = metrics[0]
    if baseline.avg_latency == 0 or baseline.actual_send_rate == 0:
        return None
    
    for i, phase in enumerate(metrics[1:], start=1):
        if phase.avg_latency == 0 or phase.actual_send_rate == 0:
            continue
        
        # Degradação em relação à baseline
        latency_increase = ((phase.avg_latency - baseline.avg_latency) / baseline.avg_latency) * 100
        loss_rate = (phase.lost_messages / phase.sent_count * 100) if phase.sent_count > 0 else 0
        
        # Queda de taxa em relação à fase anterior
        if i > 1:
            prev_phase = metrics[i-1]
            rate_drop = ((prev_phase.actual_send_rate - phase.actual_send_rate) / prev_phase.actual_send_rate) * 100
        else:
            rate_drop = 0
        
        # Critérios de threshold
        if latency_increase > 100 or loss_rate > 10 or rate_drop > 30:
            return {
                'phase_id': i,
                'target_messages': phase.target_messages,
                'actual_rate': phase.actual_send_rate,
                'latency_baseline': baseline.avg_latency,
                'latency_current': phase.avg_latency,
                'latency_increase': latency_increase,
                'loss_rate': loss_rate,
                'rate_drop': rate_drop
            }
    
    return None

def producer_thread():
    global messages_sent, start_time, phase_metrics
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1,
            batch_size=65536,  # 64KB batches
            linger_ms=1,  # Reduzido
            buffer_memory=268435456  # 256MB buffer
        )
    except Exception as e:
        print(f"❌ Erro: {e}")
        return
    
    start_time = time.time()
    threshold_found = False
    
    print("="*70)
    print("TESTE PROGRESSIVO DE THRESHOLD - MÁXIMA VELOCIDADE")
    print("="*70)
    print(f"Batch inicial: {INITIAL_BATCH:,} mensagens")
    print(f"Incremento: +{BATCH_INCREMENT:,} mensagens por fase")
    print(f"Enviando o mais rápido possível (sem limitação de taxa)\n")
    
    for phase_id in range(MAX_PHASES):
        target_messages = INITIAL_BATCH + (phase_id * BATCH_INCREMENT)
        
        phase = PhaseMetrics(phase_id, target_messages)
        phase_metrics.append(phase)
        
        print(f"📊 FASE {phase_id} | Enviando {target_messages:,} mensagens...")
        
        phase.start_time = time.time()
        
        # Enviar MÁXIMA VELOCIDADE - SEM sleep()
        for i in range(target_messages):
            try:
                message = {
                    'id': messages_sent,
                    'phase_id': phase_id,
                    'timestamp_ms': time.time() * 1000,
                    'data': 'x' * 50
                }
                producer.send('progressive_test', value=message)
                messages_sent += 1
                phase.sent_count += 1
            
            except Exception as e:
                pass
        
        producer.flush()
        phase.end_time = time.time()
        
        # Aguardar consumer processar
        time.sleep(3)
        
        # Calcular métricas
        phase_duration = phase.end_time - phase.start_time
        phase.actual_send_rate = phase.sent_count / phase_duration if phase_duration > 0 else 0
        
        if phase.latencies:
            phase.avg_latency = statistics.mean(phase.latencies)
            phase.max_latency = max(phase.latencies)
            sorted_lat = sorted(phase.latencies)
            phase.p99_latency = sorted_lat[int(len(sorted_lat) * 0.99)] if len(sorted_lat) > 10 else phase.max_latency
        
        phase.lost_messages = phase.sent_count - phase.received_count
        loss_pct = (phase.lost_messages / phase.sent_count * 100) if phase.sent_count > 0 else 0
        
        if phase_id > 0 and phase_metrics[0].avg_latency > 0:
            phase.degradation_percent = ((phase.avg_latency - phase_metrics[0].avg_latency) / 
                                        phase_metrics[0].avg_latency * 100)
        
        print(f"   ⚡ Taxa de envio: {phase.actual_send_rate:,.0f} msg/s")
        print(f"   ⏱️  Latência: avg={phase.avg_latency:.1f}ms, p99={phase.p99_latency:.1f}ms, max={phase.max_latency:.1f}ms")
        print(f"   📉 Degradação: {phase.degradation_percent:+.1f}%")
        print(f"   ❌ Perdas: {phase.lost_messages:,} ({loss_pct:.2f}%)")
        print(f"   ⏲️  Tempo da fase: {phase_duration:.2f}s\n")
        
        # Detectar threshold
        if phase_id >= 2:
            threshold = detect_threshold_progressive(phase_metrics)
            if threshold and not threshold_found:
                threshold_found = True
                print("\n" + "="*70)
                print("🚨 THRESHOLD DETECTADO!")
                print("="*70)
                print(f"Fase: {threshold['phase_id']}")
                print(f"Volume: {threshold['target_messages']:,} mensagens")
                print(f"Taxa: {threshold['actual_rate']:,.0f} msg/s")
                print(f"Latência baseline: {threshold['latency_baseline']:.1f} ms")
                print(f"Latência atual: {threshold['latency_current']:.1f} ms (+{threshold['latency_increase']:.1f}%)")
                print(f"Taxa de perda: {threshold['loss_rate']:.2f}%")
                if threshold.get('rate_drop', 0) > 0:
                    print(f"Queda de throughput: {threshold['rate_drop']:.1f}%")
                print("="*70 + "\n")
                
                # Continuar por mais 2 fases
                if phase_id > threshold['phase_id'] + 2:
                    break
    
    producer.close()
    print(f"✅ Producer finalizado: {messages_sent:,} mensagens\n")

def save_results(threshold):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'kafka_progressive_threshold_{timestamp}.txt'
    
    if start_time is None:
        return filename
    
    total_time = time.time() - start_time
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("KAFKA - TESTE PROGRESSIVO DE THRESHOLD (MÁXIMA VELOCIDADE)\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"ID: {TEST_ID}\n")
        f.write(f"Data: {datetime.now().isoformat()}\n")
        f.write(f"Tempo total: {total_time:.2f}s\n")
        f.write(f"Batch inicial: {INITIAL_BATCH:,} mensagens\n")
        f.write(f"Incremento: {BATCH_INCREMENT:,} mensagens\n\n")
        
        f.write("RESUMO\n")
        f.write("-"*80 + "\n")
        f.write(f"Total enviadas: {messages_sent:,}\n")
        f.write(f"Total recebidas: {messages_received:,}\n")
        f.write(f"Perdidas: {messages_sent - messages_received:,}\n")
        f.write(f"Throughput médio: {messages_sent / total_time:,.0f} msg/s\n\n")
        
        if threshold:
            f.write("THRESHOLD DETECTADO\n")
            f.write("-"*80 + "\n")
            f.write(f"Fase: {threshold['phase_id']}\n")
            f.write(f"Volume: {threshold['target_messages']:,} mensagens\n")
            f.write(f"Taxa: {threshold['actual_rate']:,.0f} msg/s\n")
            f.write(f"Latência baseline: {threshold['latency_baseline']:.1f} ms\n")
            f.write(f"Latência threshold: {threshold['latency_current']:.1f} ms\n")
            f.write(f"Aumento: {threshold['latency_increase']:.1f}%\n")
            f.write(f"Perda: {threshold['loss_rate']:.2f}%\n")
            if threshold.get('rate_drop'):
                f.write(f"Queda de throughput: {threshold['rate_drop']:.1f}%\n")
            f.write("\n")
        else:
            f.write("THRESHOLD NÃO DETECTADO\n")
            f.write("-"*80 + "\n")
            f.write("Sistema manteve estabilidade.\n\n")
        
        f.write("DADOS POR FASE (CSV)\n")
        f.write("-"*80 + "\n")
        f.write("phase,target_msgs,actual_rate_msg_s,sent,received,lost,avg_latency_ms,p99_latency_ms,max_latency_ms,degradation_pct,loss_pct,duration_s\n")
        
        for phase in phase_metrics:
            loss_pct = (phase.lost_messages / phase.sent_count * 100) if phase.sent_count > 0 else 0
            duration = phase.end_time - phase.start_time if phase.end_time > 0 else 0
            f.write(f"{phase.phase_id},"
                   f"{phase.target_messages},"
                   f"{phase.actual_send_rate:.2f},"
                   f"{phase.sent_count},"
                   f"{phase.received_count},"
                   f"{phase.lost_messages},"
                   f"{phase.avg_latency:.2f},"
                   f"{phase.p99_latency:.2f},"
                   f"{phase.max_latency:.2f},"
                   f"{phase.degradation_percent:.2f},"
                   f"{loss_pct:.2f},"
                   f"{duration:.2f}\n")
        
        f.write("\n" + "="*80 + "\n")
    
    print(f"💾 Resultados: '{filename}'")
    return filename

def run_test():
    global start_time, messages_sent, messages_received, phase_metrics, consumer_running
    
    messages_sent = 0
    messages_received = 0
    phase_metrics = []
    consumer_running = True
    start_time = None
    
    delete_and_create_topic()
    
    consumer_t = threading.Thread(target=consumer_thread, daemon=True)
    consumer_t.start()
    time.sleep(5)
    
    producer_t = threading.Thread(target=producer_thread)
    producer_t.start()
    producer_t.join()
    
    time.sleep(10)
    
    threshold = detect_threshold_progressive(phase_metrics)
    
    print("="*70)
    print("RESULTADOS FINAIS")
    print("="*70)
    print(f"Enviadas: {messages_sent:,}")
    print(f"Recebidas: {messages_received:,}")
    print(f"Tempo total: {time.time() - start_time:.2f}s")
    print(f"Throughput médio: {messages_sent / (time.time() - start_time):,.0f} msg/s")
    
    if threshold:
        print(f"\n🚨 Threshold na fase {threshold['phase_id']}: {threshold['target_messages']:,} msgs")
    else:
        print("\n✅ Threshold não atingido")
    print("="*70)
    
    save_results(threshold)

if __name__ == "__main__":
    run_test()
