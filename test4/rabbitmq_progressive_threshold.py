import pika
import json
import time
import threading
from datetime import datetime
import statistics

# Configuração - AJUSTADA PARA IGUALAR KAFKA
TEST_ID = int(time.time())
INITIAL_BATCH = 50000  # Igual ao Kafka
BATCH_INCREMENT = 50000  # Incremento igual
MAX_PHASES = 20
PHASE_DURATION = 3  # Tempo de espera após cada fase

# Métricas
phase_metrics = []
messages_sent = 0
messages_received = 0
send_errors = 0
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

def delete_and_create_queue():
    """Limpa e recria a fila para teste limpo"""
    try:
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost', 5672, '/', credentials)
        )
        channel = connection.channel()
        
        try:
            channel.queue_delete(queue='progressive_test')
            print("🗑️  Fila deletada")
            time.sleep(2)
        except:
            pass
        
        channel.queue_declare(queue='progressive_test', durable=True)
        print("✅ Fila criada\n")
        
        connection.close()
        time.sleep(2)
    except Exception as e:
        print(f"⚠️  {e}\n")

def consumer_thread():
    """Consumer thread - processa mensagens e calcula latências"""
    global messages_received, consumer_running
    
    def callback(ch, method, properties, body):
        global messages_received
        
        try:
            t_receive = time.time() * 1000
            msg = json.loads(body)
            t_sent = msg['timestamp_ms']
            phase_id = msg['phase_id']
            latency = t_receive - t_sent
            
            if phase_id < len(phase_metrics):
                phase_metrics[phase_id].latencies.append(latency)
                phase_metrics[phase_id].received_count += 1
            
            messages_received += 1
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        except Exception as e:
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    try:
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                'localhost', 
                5672, 
                '/',
                credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
        )
        channel = connection.channel()
        
        channel.queue_declare(queue='progressive_test', durable=True)
        channel.basic_qos(prefetch_count=1000)  # Igual ao Kafka
        
        channel.basic_consume(
            queue='progressive_test',
            on_message_callback=callback
        )
        
        print("🎧 Consumer iniciado\n")
        
        while consumer_running:
            try:
                connection.process_data_events(time_limit=1)
            except:
                break
        
        # Processar mensagens restantes
        for _ in range(15):
            try:
                connection.process_data_events(time_limit=1)
            except:
                break
        
        connection.close()
    
    except Exception as e:
        print(f"⚠️  Consumer finalizado: {e}")
    finally:
        consumer_running = False

def detect_threshold_progressive(metrics):
    """
    Detecta threshold com MESMOS critérios do Kafka:
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
        
        # Critérios de threshold IGUAIS AO KAFKA
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
    """Producer thread - envia mensagens em fases progressivas"""
    global messages_sent, send_errors, start_time, phase_metrics, consumer_running
    
    try:
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                'localhost',
                5672,
                '/',
                credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
        )
        channel = connection.channel()
        
        channel.queue_declare(queue='progressive_test', durable=True)
        
    except Exception as e:
        print(f"❌ Erro ao conectar producer: {e}")
        return
    
    start_time = time.time()
    threshold_found = False
    
    print("="*70)
    print("TESTE PROGRESSIVO DE THRESHOLD - RABBITMQ - MÁXIMA VELOCIDADE")
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
        
        # Enviar MÁXIMA VELOCIDADE - SEM sleep() entre mensagens
        for i in range(target_messages):
            try:
                message = {
                    'id': messages_sent,
                    'phase_id': phase_id,
                    'timestamp_ms': time.time() * 1000,
                    'data': 'x' * 50
                }
                
                channel.basic_publish(
                    exchange='',
                    routing_key='progressive_test',
                    body=json.dumps(message),
                    properties=pika.BasicProperties(
                        delivery_mode=1  # Non-persistent (igual ao Kafka acks=1)
                    )
                )
                messages_sent += 1
                phase.sent_count += 1
            
            except Exception as e:
                send_errors += 1
                # Tentar reconectar se necessário
                try:
                    connection = pika.BlockingConnection(
                        pika.ConnectionParameters('localhost', 5672, '/', credentials)
                    )
                    channel = connection.channel()
                except:
                    pass
        
        phase.end_time = time.time()
        
        # Aguardar consumer processar (igual ao Kafka)
        time.sleep(PHASE_DURATION)
        
        # Calcular métricas da fase
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
    
    connection.close()
    consumer_running = False
    print(f"✅ Producer finalizado: {messages_sent:,} mensagens\n")

def save_results(threshold):
    """Salva resultados em arquivo com formato igual ao Kafka"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'rabbitmq_progressive_threshold_{timestamp}.txt'
    
    if start_time is None:
        return filename
    
    total_time = time.time() - start_time
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("RABBITMQ - TESTE PROGRESSIVO DE THRESHOLD (MÁXIMA VELOCIDADE)\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"ID: {TEST_ID}\n")
        f.write(f"Data: {datetime.now().isoformat()}\n")
        f.write(f"Tempo total: {total_time:.2f}s\n")
        f.write(f"Batch inicial: {INITIAL_BATCH:,} mensagens\n")
        f.write(f"Incremento: {BATCH_INCREMENT:,} mensagens\n")
        f.write(f"Erros de envio: {send_errors}\n\n")
        
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
    """Executa o teste completo"""
    global start_time, messages_sent, messages_received, send_errors, phase_metrics, consumer_running
    
    # Reset de variáveis globais
    messages_sent = 0
    messages_received = 0
    send_errors = 0
    phase_metrics = []
    consumer_running = True
    start_time = None
    
    delete_and_create_queue()
    
    # Iniciar consumer
    consumer_t = threading.Thread(target=consumer_thread, daemon=True)
    consumer_t.start()
    time.sleep(5)
    
    # Iniciar producer
    producer_t = threading.Thread(target=producer_thread)
    producer_t.start()
    producer_t.join()
    
    # Aguardar processamento final
    time.sleep(10)
    consumer_running = False
    
    threshold = detect_threshold_progressive(phase_metrics)
    
    print("="*70)
    print("RESULTADOS FINAIS")
    print("="*70)
    print(f"Enviadas: {messages_sent:,}")
    print(f"Recebidas: {messages_received:,}")
    print(f"Erros: {send_errors}")
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
