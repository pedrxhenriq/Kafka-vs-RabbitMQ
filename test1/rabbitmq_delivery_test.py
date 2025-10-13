import pika
import json
import time
import sys

def producer():
    """Envia 50 mensagens para a fila"""
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    queue_name = 'delivery_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    print("=" * 60)
    print("TESTE DE GARANTIA DE ENTREGA - RABBITMQ")
    print("=" * 60)
    print("\nEnviando 50 mensagens para a fila...")
    
    for i in range(50):
        message = {
            'id': i,
            'data': f'Mensagem número {i}',
            'timestamp': time.time()
        }
        
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensagem persistente
            )
        )
        
        if (i + 1) % 10 == 0:
            print(f"  ✓ {i + 1} mensagens enviadas")
    
    print(f"\n✓ Total: 50 mensagens enviadas com sucesso!")
    print("\nAgora execute o consumidor:")
    print("  python rabbitmq_delivery_test.py consumer")
    connection.close()


def consumer_phase1():
    """Consome apenas 20 mensagens e simula interrupção"""
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    queue_name = 'delivery_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    messages_received = []
    
    print("\n" + "=" * 60)
    print("FASE 1: CONSUMINDO MENSAGENS (será interrompido)")
    print("=" * 60)
    
    def callback(ch, method, properties, body):
        message = json.loads(body)
        messages_received.append(message['id'])
        print(f"  ✓ Recebida mensagem {message['id']}")
        
        # Confirmar recebimento (ACK)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        # Simular processamento
        time.sleep(0.1)
        
        # Interromper após 20 mensagens
        if len(messages_received) >= 20:
            print(f"\n⚠️  SIMULANDO INTERRUPÇÃO DO CONSUMIDOR!")
            print(f"    Mensagens processadas: {len(messages_received)}")
            print(f"    IDs processados: {messages_received}")
            ch.stop_consuming()
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    
    print("Iniciando consumo...")
    channel.start_consuming()
    connection.close()
    
    return messages_received


def consumer_phase2(first_batch_ids):
    """Reconecta e consome as mensagens restantes"""
    print("\n" + "=" * 60)
    print("FASE 2: RECONECTANDO CONSUMIDOR")
    print("=" * 60)
    print("Aguardando 3 segundos antes de reconectar...")
    time.sleep(3)
    
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    queue_name = 'delivery_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    messages_received = []
    
    def callback(ch, method, properties, body):
        message = json.loads(body)
        messages_received.append(message['id'])
        print(f"  ✓ Recebida mensagem {message['id']}")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        time.sleep(0.1)
        
        # Parar após receber todas as mensagens restantes
        if len(messages_received) >= 30:
            ch.stop_consuming()
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    
    print("Consumindo mensagens pendentes...")
    channel.start_consuming()
    connection.close()
    
    # Análise dos resultados
    print("\n" + "=" * 60)
    print("RESULTADOS DO TESTE - RABBITMQ")
    print("=" * 60)
    print(f"\nFASE 1 (antes da interrupção):")
    print(f"  • Mensagens processadas: {len(first_batch_ids)}")
    print(f"  • IDs: {first_batch_ids}")
    
    print(f"\nFASE 2 (após reconexão):")
    print(f"  • Mensagens processadas: {len(messages_received)}")
    print(f"  • IDs: {messages_received}")
    
    print(f"\nTOTAL:")
    print(f"  • Mensagens únicas recebidas: {len(first_batch_ids) + len(messages_received)}")
    
    # Verificar se houve reprocessamento
    all_ids = first_batch_ids + messages_received
    if len(all_ids) != len(set(all_ids)):
        print(f"  ⚠️  ATENÇÃO: Houve reprocessamento de mensagens!")
        duplicates = [id for id in all_ids if all_ids.count(id) > 1]
        print(f"  • IDs duplicados: {set(duplicates)}")
    else:
        print(f"  ✓ Nenhuma mensagem foi reprocessada")
    
    # Verificar mensagens perdidas
    expected_ids = set(range(50))
    received_ids = set(all_ids)
    missing_ids = expected_ids - received_ids
    
    if missing_ids:
        print(f"  ⚠️  Mensagens perdidas: {sorted(missing_ids)}")
    else:
        print(f"  ✓ Todas as 50 mensagens foram entregues")
    
    print("\nCONCLUSÃO:")
    print("  RabbitMQ mantém mensagens não processadas na fila.")
    print("  Após reconexão, o consumidor recebe apenas mensagens pendentes.")
    print("=" * 60)
    
    # Salvar resultados
    with open('rabbitmq_delivery_results.txt', 'w') as f:
        f.write("=== TESTE DE GARANTIA DE ENTREGA - RABBITMQ ===\n\n")
        f.write(f"Fase 1 (antes interrupção): {len(first_batch_ids)} mensagens\n")
        f.write(f"IDs Fase 1: {first_batch_ids}\n\n")
        f.write(f"Fase 2 (após reconexão): {len(messages_received)} mensagens\n")
        f.write(f"IDs Fase 2: {messages_received}\n\n")
        f.write(f"Total único: {len(set(all_ids))} mensagens\n")
        f.write(f"Mensagens reprocessadas: {len(all_ids) - len(set(all_ids))}\n")
        f.write(f"Mensagens perdidas: {len(missing_ids)}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso:")
        print("  python rabbitmq_delivery_test.py producer   - Envia mensagens")
        print("  python rabbitmq_delivery_test.py consumer   - Consome com interrupção")
        sys.exit(1)
    
    if sys.argv[1] == 'producer':
        producer()
    elif sys.argv[1] == 'consumer':
        first_batch = consumer_phase1()
        consumer_phase2(first_batch)
    else:
        print("Argumento inválido! Use 'producer' ou 'consumer'")