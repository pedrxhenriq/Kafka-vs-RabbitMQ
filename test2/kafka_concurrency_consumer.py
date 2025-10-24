from kafka import KafkaConsumer, TopicPartition
import json
import time
import sys
import statistics
from datetime import datetime

class ConcurrencyConsumer:
    def __init__(self, consumer_id):
        self.consumer_id = consumer_id
        self.messages_received = []
        self.latencies = []
        self.start_time = None
        self.end_time = None
        self.partitions_processed = set()
        
    def run(self):
        """Iniciar consumer"""
        
        topic_name = 'concurrency_test'
        partition_id = self.consumer_id - 1
        
        consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=60000
        )
        
        print("=" * 70)
        print(f"KAFKA - CONSUMER {self.consumer_id} INICIADO")
        print("=" * 70)
        print(f"🎧 Conectando ao tópico '{topic_name}'...")
        print(f"📍 Partição fixa: {partition_id}")
        
        partition = TopicPartition(topic_name, partition_id)
        consumer.assign([partition])
        
        # Verificar partições atribuídas
        assigned_partitions = consumer.assignment()
        print(f"✅ [Consumer {self.consumer_id}] Partições atribuídas: {assigned_partitions}")
        print(f"🟢 [Consumer {self.consumer_id}] PRONTO para receber mensagens!\n")
        
        try:
            for message in consumer:
                if self.start_time is None:
                    self.start_time = time.time()
                    print(f"[Consumer {self.consumer_id}] Primeira mensagem recebida!")
                    print(f"[Consumer {self.consumer_id}] Partições atribuídas: {consumer.assignment()}\n")
                
                # Timestamp de recebimento
                receive_time = time.time() * 1000
                
                # Dados da mensagem
                data = message.value
                send_time = data['timestamp_ms']
                latency = receive_time - send_time
                
                # Registrar partição processada
                self.partitions_processed.add(message.partition)
                
                # Armazenar dados
                self.messages_received.append({
                    'message_id': data['id'],
                    'sequence': data['sequence'],
                    'partition': message.partition,
                    'offset': message.offset,
                    'timestamp_send': send_time,
                    'timestamp_receive': receive_time,
                    'latency_ms': latency,
                    'consumer_id': self.consumer_id
                })
                self.latencies.append(latency)
                
                # Log a cada 500 mensagens
                if len(self.messages_received) % 500 == 0:
                    print(f"[Consumer {self.consumer_id}] Processadas {len(self.messages_received)} mensagens "
                          f"| Partições: {sorted(self.partitions_processed)}")
                
                self.end_time = time.time()
        
        except KeyboardInterrupt:
            print(f"\n⚠️  [Consumer {self.consumer_id}] Interrompido pelo usuário")
        except Exception as e:
            print(f"⚠️  [Consumer {self.consumer_id}] Timeout ou erro: {e}")
        
        consumer.close()
        self.save_results()
    
    def save_results(self):
        """Salvar resultados em arquivo"""
        
        if not self.messages_received:
            print(f"⚠️  [Consumer {self.consumer_id}] Nenhuma mensagem recebida!")
            return
        
        total_messages = len(self.messages_received)
        processing_time = self.end_time - self.start_time if self.end_time else 0
        
        # Análise de partições
        partition_counts = {}
        for msg in self.messages_received:
            partition_counts[msg['partition']] = partition_counts.get(msg['partition'], 0) + 1
        
        print(f"\n{'=' * 70}")
        print(f"RESULTADOS - CONSUMER {self.consumer_id}")
        print("=" * 70)
        print(f"📊 Total de mensagens: {total_messages}")
        print(f"⏱️  Tempo de processamento: {processing_time:.2f}s")
        print(f"📈 Taxa: {total_messages/processing_time:.0f} msgs/s")
        print(f"🗂️  Partições processadas: {sorted(self.partitions_processed)}")
        print(f"📊 Distribuição por partição: {partition_counts}")
        print(f"⏱️  Latência média: {statistics.mean(self.latencies):.2f} ms")
        print(f"⚡ Latência mínima: {min(self.latencies):.2f} ms")
        print(f"🐌 Latência máxima: {max(self.latencies):.2f} ms")
        print("=" * 70)
        
        # Salvar arquivo
        filename = f'kafka_consumer_{self.consumer_id}_results.txt'
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("=" * 70 + "\n")
            f.write(f"KAFKA - RESULTADOS DO CONSUMER {self.consumer_id}\n")
            f.write("=" * 70 + "\n\n")
            
            f.write("📊 RESUMO\n")
            f.write("-" * 70 + "\n")
            f.write(f"Consumer ID: {self.consumer_id}\n")
            f.write(f"Grupo de consumo: concurrency_test_group\n")
            f.write(f"Total de mensagens processadas: {total_messages}\n")
            f.write(f"Partições processadas: {sorted(self.partitions_processed)}\n")
            f.write(f"Distribuição por partição:\n")
            for part, count in sorted(partition_counts.items()):
                f.write(f"  Partição {part}: {count} mensagens\n")
            f.write(f"\nTempo de processamento: {processing_time:.2f} segundos\n")
            f.write(f"Taxa de processamento: {total_messages/processing_time:.0f} msgs/s\n")
            f.write(f"Latência média: {statistics.mean(self.latencies):.2f} ms\n")
            f.write(f"Latência mínima: {min(self.latencies):.2f} ms\n")
            f.write(f"Latência máxima: {max(self.latencies):.2f} ms\n")
            f.write(f"Mediana: {statistics.median(self.latencies):.2f} ms\n")
            f.write(f"Desvio padrão: {statistics.stdev(self.latencies):.2f} ms\n")
            f.write("\n" + "=" * 70 + "\n\n")
            
            f.write("📋 MENSAGENS PROCESSADAS\n")
            f.write("-" * 70 + "\n")
            f.write(f"{'Msg_ID':>7} | {'Seq':>8} | {'Part':>4} | {'Offset':>7} | {'Latency_ms':>12}\n")
            f.write("-" * 70 + "\n")
            
            for msg in self.messages_received:
                f.write(f"{msg['message_id']:7d} | "
                       f"{msg['sequence']:8d} | "
                       f"{msg['partition']:4d} | "
                       f"{msg['offset']:7d} | "
                       f"{msg['latency_ms']:12.3f}\n")
            
            f.write("\n" + "=" * 70 + "\n")
        
        print(f"💾 Resultados salvos em '{filename}'")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        consumer_id = int(sys.argv[1])
    else:
        consumer_id = 1
    
    consumer = ConcurrencyConsumer(consumer_id)
    consumer.run()