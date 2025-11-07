import pika
import json
import time
from datetime import datetime

def produce_messages():
    """Envia 50 mensagens para o RabbitMQ"""
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    queue_name = 'retention_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    print("\n" + "="*60)
    print("RABBITMQ - ENVIANDO MENSAGENS")
    print("="*60)
    
    for i in range(50):
        message = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'content': f'Mensagem de teste #{i}'
        }
        
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        if (i + 1) % 10 == 0:
            print(f"✓ Enviadas {i+1}/50 mensagens")
    
    connection.close()
    print("✅ 50 mensagens enviadas!\n")

def consume_first_time():
    """Primeira leitura - consome e confirma (ACK)"""
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    queue_name = 'retention_test'
    channel.queue_declare(queue=queue_name, durable=True)
    
    print("="*60)
    print("RABBITMQ - PRIMEIRA LEITURA")
    print("="*60)
    
    messages_first = []
    
    method_frame, header_frame, body = channel.basic_get(queue=queue_name)
    
    while method_frame:
        msg = json.loads(body)
        messages_first.append({
            'id': msg['id'],
            'timestamp': msg['timestamp'],
            'read_at': datetime.now().isoformat(),
            'delivery_tag': method_frame.delivery_tag
        })
        
        print(f"📨 Msg {msg['id']:2d} | Delivery Tag: {method_frame.delivery_tag}")
        
        # ACK: confirma e REMOVE a mensagem da fila
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        
        method_frame, header_frame, body = channel.basic_get(queue=queue_name)
    
    connection.close()
    print(f"✅ Primeira leitura: {len(messages_first)} mensagens (REMOVIDAS da fila)\n")
    return messages_first

def attempt_second_read():
    """Tentativa de segunda leitura - fila estará vazia"""
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', 5672, '/', credentials)
    )
    channel = connection.channel()
    
    queue_name = 'retention_test'
    
    print("="*60)
    print("RABBITMQ - TENTATIVA DE SEGUNDA LEITURA")
    print("="*60)
    print("🔍 Tentando reprocessar mensagens...\n")
    
    messages_second = []
    
    method_frame, header_frame, body = channel.basic_get(queue=queue_name)
    
    if method_frame:
        while method_frame:
            msg = json.loads(body)
            messages_second.append({
                'id': msg['id'],
                'timestamp': msg['timestamp'],
                'read_at': datetime.now().isoformat()
            })
            print(f"📨 Msg {msg['id']:2d}")
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            method_frame, header_frame, body = channel.basic_get(queue=queue_name)
    else:
        print("❌ FILA VAZIA - Mensagens não podem ser reprocessadas!")
        print("⚠️  RabbitMQ remove mensagens após ACK\n")
    
    connection.close()
    return messages_second

def save_results(first_read, second_read):
    """Salva resultados em arquivo"""
    with open('rabbitmq_retention_results.txt', 'w', encoding='utf-8') as f:
        f.write("="*70 + "\n")
        f.write("RABBITMQ - TESTE DE RETENÇÃO E REPROCESSAMENTO\n")
        f.write("="*70 + "\n\n")
        
        f.write("📖 DESCRIÇÃO DO TESTE:\n")
        f.write("-"*70 + "\n")
        f.write("1. Enviadas 50 mensagens para a fila 'retention_test'\n")
        f.write("2. Primeira leitura: consumidas todas as mensagens com ACK\n")
        f.write("3. Tentativa de segunda leitura: buscar mensagens novamente\n")
        f.write("4. Resultado: RabbitMQ remove mensagens após ACK (fila vazia)\n\n")
        
        f.write("="*70 + "\n")
        f.write("PRIMEIRA LEITURA\n")
        f.write("="*70 + "\n")
        f.write(f"{'ID':>4} | {'Delivery Tag':>12} | {'Timestamp':>26} | {'Read At':>26}\n")
        f.write("-"*70 + "\n")
        for msg in first_read:
            f.write(f"{msg['id']:4d} | {msg['delivery_tag']:12d} | "
                   f"{msg['timestamp']:>26} | {msg['read_at']:>26}\n")
        
        f.write("\n" + "="*70 + "\n")
        f.write("SEGUNDA LEITURA (TENTATIVA DE REPROCESSAMENTO)\n")
        f.write("="*70 + "\n")
        
        if second_read:
            f.write(f"{'ID':>4} | {'Timestamp':>26} | {'Read At':>26}\n")
            f.write("-"*70 + "\n")
            for msg in second_read:
                f.write(f"{msg['id']:4d} | {msg['timestamp']:>26} | {msg['read_at']:>26}\n")
        else:
            f.write("❌ FILA VAZIA - Nenhuma mensagem disponível\n")
            f.write("⚠️  Mensagens foram REMOVIDAS após ACK na primeira leitura\n")
        
        f.write("\n" + "="*70 + "\n")
        f.write("RESULTADO RABBITMQ:\n")
        f.write("-"*70 + "\n")
        f.write(f"❌ Mensagens NÃO persistidas após ACK\n")
        f.write(f"❌ Reprocessamento impossível: 0 mensagens na segunda leitura\n")
        f.write(f"❌ Fila esvaziada após confirmação de recebimento\n")
        f.write(f"⚠️  Modelo de entrega única - sem histórico\n")
        f.write("="*70 + "\n")
    
    print("💾 Resultados salvos em 'rabbitmq_retention_results.txt'")

if __name__ == "__main__":
    print("\n🚀 INICIANDO TESTE DE RETENÇÃO - RABBITMQ\n")
    
    produce_messages()
    time.sleep(2)
    
    first = consume_first_time()
    time.sleep(2)
    second = attempt_second_read()
    
    save_results(first, second)
    
    print("\n✅ TESTE CONCLUÍDO!")
