import pika
import json
import time
import sys
from datetime import datetime
import os

QUEUE_NAME = 'concurrency_test'

def create_queue():
    """Cria a fila se não existir"""
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    # Deletar fila antiga se existir (para começar limpo)
    try:
        channel.queue_delete(queue=QUEUE_NAME)
        print(f"Fila '{QUEUE_NAME}' deletada (limpeza)")
    except:
        pass
    
    # Criar fila durável
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    print(f"Fila '{QUEUE_NAME}' criada com sucesso")
    
    connection.close()

def producer(num_messages=1000):
    """Produz N mensagens sequenciais"""
    create_queue()
    
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    print("\n" + "=" * 70)
    print("TESTE DE CONCORRÊNCIA - RABBITMQ - PRODUCER")
    print("=" * 70)
    print(f"\nEnviando {num_messages} mensagens para a fila '{QUEUE_NAME}'...")
    
    start_time = time.time()
    
    for i in range(num_messages):
        message = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'data': f'Mensagem número {i}'
        }
        
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensagem persistente
            )
        )
        
        if (i + 1) % 100 == 0:
            print(f"  ✓ {i + 1} mensagens enviadas")
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f"\n✓ Total: {num_messages} mensagens enviadas com sucesso!")
    print(f"  Tempo total: {elapsed:.2f} segundos")
    print(f"  Taxa: {num_messages/elapsed:.2f} msgs/segundo")
    
    print("\n" + "=" * 70)
    print("Agora execute 3 consumidores em terminais diferentes:")
    print("  Terminal 1: python rabbitmq_concurrency_test.py consumer C1")
    print("  Terminal 2: python rabbitmq_concurrency_test.py consumer C2")
    print("  Terminal 3: python rabbitmq_concurrency_test.py consumer C3")
    print("=" * 70)
    
    connection.close()

def consumer(consumer_id):
    """Consome mensagens e registra detalhes"""
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    
    # Preparar arquivo de log
    log_filename = f'rabbitmq_consumer_{consumer_id}.log'
    
    # Estado do consumidor
    state = {
        'consumer_id': consumer_id,
        'messages_received': [],
        'timestamps': [],
        'start_time': None,
        'end_time': None
    }
    
    print("\n" + "=" * 70)
    print(f"CONSUMIDOR {consumer_id} - RABBITMQ - INICIADO")
    print("=" * 70)
    print(f"Aguardando mensagens na fila '{QUEUE_NAME}'...")
    print(f"Logs sendo salvos em: {log_filename}")
    print("Pressione CTRL+C para parar\n")
    
    # Abrir arquivo de log
    log_file = open(log_filename, 'w', encoding='utf-8')
    log_file.write(f"=== Consumidor {consumer_id} - RabbitMQ - Iniciado em {datetime.now().isoformat()} ===\n")
    log_file.write(f"Plataforma: RabbitMQ\n")
    log_file.write(f"Fila: {QUEUE_NAME}\n")
    log_file.write(f"Prefetch: 1\n\n")
    
    def callback(ch, method, properties, body):
        if state['start_time'] is None:
            state['start_time'] = time.time()
        
        receive_time = datetime.now()
        message = json.loads(body)
        msg_id = message['id']
        
        state['messages_received'].append(msg_id)
        state['timestamps'].append(receive_time.isoformat())
        
        # Log detalhado
        log_line = f"[{receive_time.strftime('%H:%M:%S.%f')[:-3]}] Recebida mensagem ID={msg_id}\n"
        log_file.write(log_line)
        log_file.flush()  # Forçar escrita imediata
        
        # Print a cada 100 mensagens
        if len(state['messages_received']) % 100 == 0:
            print(f"  ✓ {len(state['messages_received'])} mensagens processadas")
        
        # ACK manual
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # Configurar prefetch e consumo
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n\n⚠️  Consumidor interrompido pelo usuário")
    finally:
        state['end_time'] = time.time()
        
        # Estatísticas finais
        total_messages = len(state['messages_received'])
        elapsed = state['end_time'] - state['start_time'] if state['start_time'] else 0
        rate = total_messages / elapsed if elapsed > 0 else 0
        
        print("\n" + "=" * 70)
        print(f"CONSUMIDOR {consumer_id} - ESTATÍSTICAS FINAIS")
        print("=" * 70)
        print(f"Total processado: {total_messages} mensagens")
        print(f"Tempo total: {elapsed:.2f} segundos")
        print(f"Taxa: {rate:.2f} msgs/segundo")
        print(f"IDs processados: {sorted(state['messages_received'][:10])}... (primeiros 10)")
        print("=" * 70)
        
        # Salvar resumo no log
        log_file.write(f"\n=== Consumidor {consumer_id} - Finalizado em {datetime.now().isoformat()} ===\n")
        log_file.write(f"Total processado: {total_messages} mensagens\n")
        log_file.write(f"Tempo total: {elapsed:.2f} segundos\n")
        log_file.write(f"Taxa: {rate:.2f} msgs/segundo\n")
        log_file.write(f"IDs processados (ordenados): {sorted(state['messages_received'])}\n")
        
        log_file.close()
        connection.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python rabbitmq_concurrency_test.py producer [num_messages]  - Envia mensagens (padrão: 1000)")
        print("  python rabbitmq_concurrency_test.py consumer <consumer_id>  - Consome mensagens")
        print("\nExemplo:")
        print("  python rabbitmq_concurrency_test.py producer 1000")
        print("  python rabbitmq_concurrency_test.py consumer C1")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'producer':
        num_msgs = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
        producer(num_msgs)
    elif command == 'consumer':
        if len(sys.argv) < 3:
            print("Erro: Especifique o ID do consumidor (ex: C1, C2, C3)")
            sys.exit(1)
        consumer_id = sys.argv[2]
        consumer(consumer_id)
    else:
        print("Comando inválido! Use 'producer' ou 'consumer'")
