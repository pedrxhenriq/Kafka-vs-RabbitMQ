import pika
import json
import time
import sys
import statistics
from datetime import datetime
from collections import defaultdict

class ConcurrencyConsumer:
    def __init__(self, consumer_id):
        self.consumer_id = consumer_id
        self.messages_received = []
        self.latencies = []
        self.start_time = None
        self.end_time = None
        
    def callback(self, ch, method, properties, body):
        """Callback para processar cada mensagem recebida"""
        
        if self.start_time is None:
            self.start_time = time.time()
        
        # Timestamp de recebimento
        receive_time = time.time() * 1000
        
        # Desserializar mensagem
        message = json.loads(body)
        
        # Calcular latência
        send_time = message['timestamp_ms']
        latency = receive_time - send_time
        
        # Armazenar dados
        self.messages_received.append({
            'message_id': message['id'],
            'sequence': message['sequence'],
            'timestamp_send': send_time,
            'timestamp_receive': receive_time,
            'latency_ms': latency,
            'consumer_id': self.consumer_id
        })
        self.latencies.append(latency)
        
        # Log a cada 500 mensagens
        if len(self.messages_received) % 500 == 0:
            print(f"[Consumer {self.consumer_id}] Processadas {len(self.messages_received)} mensagens")
        
        # ACK da mensagem
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        self.end_time = time.time()
    
    def run(self):
        """Iniciar consumer"""
        
        # Conectar ao RabbitMQ
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost', 5672, '/', credentials)
        )
        channel = connection.channel()
        
        # Declarar fila
        queue_name = 'concurrency_test'
        channel.queue_declare(queue=queue_name, durable=True)
        
        # QoS: 1 mensagem por vez (fair dispatch)
        channel.basic_qos(prefetch_count=1)
        
        # Registrar callback
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.callback
        )
        
        print("=" * 70)
        print(f"RABBITMQ - CONSUMER {self.consumer_id} INICIADO")
        print("=" * 70)
        print("🎧 Aguardando mensagens... Pressione CTRL+C para sair\n")
        
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            print(f"\n⚠️  [Consumer {self.consumer_id}] Interrompido pelo usuário")
            channel.stop_consuming()
        
        connection.close()
        self.save_results()
    
    def save_results(self):
        """Salvar resultados em arquivo"""
        
        if not self.messages_received:
            print(f"⚠️  [Consumer {self.consumer_id}] Nenhuma mensagem recebida!")
            return
        
        total_messages = len(self.messages_received)
        processing_time = self.end_time - self.start_time if self.end_time else 0
        
        print(f"\n{'=' * 70}")
        print(f"RESULTADOS - CONSUMER {self.consumer_id}")
        print("=" * 70)
        print(f"📊 Total de mensagens: {total_messages}")
        print(f"⏱️  Tempo de processamento: {processing_time:.2f}s")
        print(f"📈 Taxa: {total_messages/processing_time:.0f} msgs/s")
        print(f"⏱️  Latência média: {statistics.mean(self.latencies):.2f} ms")
        print(f"⚡ Latência mínima: {min(self.latencies):.2f} ms")
        print(f"🐌 Latência máxima: {max(self.latencies):.2f} ms")
        print("=" * 70)
        
        # Salvar arquivo detalhado
        filename = f'rabbitmq_consumer_{self.consumer_id}_results.txt'
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("=" * 70 + "\n")
            f.write(f"RABBITMQ - RESULTADOS DO CONSUMER {self.consumer_id}\n")
            f.write("=" * 70 + "\n\n")
            
            f.write("📊 RESUMO\n")
            f.write("-" * 70 + "\n")
            f.write(f"Consumer ID: {self.consumer_id}\n")
            f.write(f"Total de mensagens processadas: {total_messages}\n")
            f.write(f"Tempo de processamento: {processing_time:.2f} segundos\n")
            f.write(f"Taxa de processamento: {total_messages/processing_time:.0f} msgs/s\n")
            f.write(f"Latência média: {statistics.mean(self.latencies):.2f} ms\n")
            f.write(f"Latência mínima: {min(self.latencies):.2f} ms\n")
            f.write(f"Latência máxima: {max(self.latencies):.2f} ms\n")
            f.write(f"Mediana: {statistics.median(self.latencies):.2f} ms\n")
            f.write(f"Desvio padrão: {statistics.stdev(self.latencies):.2f} ms\n")
            f.write("\n" + "=" * 70 + "\n\n")
            
            f.write("📋 MENSAGENS PROCESSADAS\n")
            f.write("-" * 70 + "\n")
            f.write(f"{'Msg_ID':>7} | {'Sequence':>8} | {'Latency_ms':>12}\n")
            f.write("-" * 70 + "\n")
            
            for msg in self.messages_received:
                f.write(f"{msg['message_id']:7d} | "
                       f"{msg['sequence']:8d} | "
                       f"{msg['latency_ms']:12.3f}\n")
            
            f.write("\n" + "=" * 70 + "\n")
        
        print(f"💾 Resultados salvos em '{filename}'")

if __name__ == "__main__":
    # Pegar ID do consumer via argumento (1, 2 ou 3)
    if len(sys.argv) > 1:
        consumer_id = int(sys.argv[1])
    else:
        consumer_id = 1
    
    consumer = ConcurrencyConsumer(consumer_id)
    consumer.run()