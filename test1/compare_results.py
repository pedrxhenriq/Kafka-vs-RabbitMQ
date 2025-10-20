import os
import re
import statistics

def extract_metrics(filename):
    """
    Extrai métricas de um arquivo de resultados.
    """
    if not os.path.exists(filename):
        return None
    
    metrics = {}
    latencies = []
    
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
        
        # Extrair estatísticas usando regex
        avg_match = re.search(r'Latência média:\s+([\d.]+)\s+ms', content)
        min_match = re.search(r'Latência mínima:\s+([\d.]+)\s+ms', content)
        max_match = re.search(r'Latência máxima:\s+([\d.]+)\s+ms', content)
        median_match = re.search(r'Mediana:\s+([\d.]+)\s+ms', content)
        std_match = re.search(r'Desvio padrão:\s+([\d.]+)\s+ms', content)
        p50_match = re.search(r'P50:\s+([\d.]+)\s+ms', content)
        p90_match = re.search(r'P90:\s+([\d.]+)\s+ms', content)
        p95_match = re.search(r'P95:\s+([\d.]+)\s+ms', content)
        p99_match = re.search(r'P99:\s+([\d.]+)\s+ms', content)
        
        if avg_match:
            metrics['avg'] = float(avg_match.group(1))
        if min_match:
            metrics['min'] = float(min_match.group(1))
        if max_match:
            metrics['max'] = float(max_match.group(1))
        if median_match:
            metrics['median'] = float(median_match.group(1))
        if std_match:
            metrics['std'] = float(std_match.group(1))
        if p50_match:
            metrics['p50'] = float(p50_match.group(1))
        if p90_match:
            metrics['p90'] = float(p90_match.group(1))
        if p95_match:
            metrics['p95'] = float(p95_match.group(1))
        if p99_match:
            metrics['p99'] = float(p99_match.group(1))
    
    return metrics

def compare_brokers():
    """
    Compara os resultados de latência entre RabbitMQ e Kafka.
    """
    
    print("=" * 80)
    print("COMPARAÇÃO DE LATÊNCIA: RABBITMQ vs KAFKA")
    print("=" * 80)
    print()
    
    # Carregar métricas
    rabbitmq_metrics = extract_metrics('test1/results/rabbitmq_latency_results5.txt')
    kafka_metrics = extract_metrics('test1/results/kafka_latency_results5.txt')
    
    # Verificar se os arquivos existem
    if not rabbitmq_metrics:
        print("⚠️  Arquivo 'rabbitmq_latency_results.txt' não encontrado!")
        print("   Execute o consumer do RabbitMQ primeiro.\n")
    
    if not kafka_metrics:
        print("⚠️  Arquivo 'kafka_latency_results.txt' não encontrado!")
        print("   Execute o consumer do Kafka primeiro.\n")
    
    if not rabbitmq_metrics or not kafka_metrics:
        print("❌ Não é possível fazer a comparação sem ambos os arquivos.")
        return
    
    # Exibir comparação lado a lado
    print(f"{'Métrica':<20} | {'RabbitMQ':>15} | {'Kafka':>15} | {'Diferença':>15} | {'Vencedor':<10}")
    print("-" * 80)
    
    metrics_to_compare = [
        ('Latência Média', 'avg'),
        ('Latência Mínima', 'min'),
        ('Latência Máxima', 'max'),
        ('Mediana', 'median'),
        ('Desvio Padrão', 'std'),
        ('Percentil 50', 'p50'),
        ('Percentil 90', 'p90'),
        ('Percentil 95', 'p95'),
        ('Percentil 99', 'p99'),
    ]
    
    wins = {'rabbitmq': 0, 'kafka': 0}
    
    for label, key in metrics_to_compare:
        rabbitmq_val = rabbitmq_metrics.get(key, 0)
        kafka_val = kafka_metrics.get(key, 0)
        diff = kafka_val - rabbitmq_val
        diff_percent = (diff / rabbitmq_val * 100) if rabbitmq_val > 0 else 0
        
        # Determinar vencedor (menor é melhor)
        if rabbitmq_val < kafka_val:
            winner = "🏆 RabbitMQ"
            wins['rabbitmq'] += 1
        elif kafka_val < rabbitmq_val:
            winner = "🏆 Kafka"
            wins['kafka'] += 1
        else:
            winner = "🤝 Empate"
        
        print(f"{label:<20} | {rabbitmq_val:>12.2f} ms | {kafka_val:>12.2f} ms | "
              f"{diff:>+12.2f} ms | {winner:<10}")
    
    # Resumo final
    print("=" * 80)
    print("\n📊 RESUMO DA COMPARAÇÃO:")
    print("-" * 80)
    
    if wins['rabbitmq'] > wins['kafka']:
        print(f"🏆 VENCEDOR: RabbitMQ ({wins['rabbitmq']} métricas melhores)")
        print(f"   RabbitMQ é mais rápido em {wins['rabbitmq']} de {len(metrics_to_compare)} métricas")
    elif wins['kafka'] > wins['rabbitmq']:
        print(f"🏆 VENCEDOR: Kafka ({wins['kafka']} métricas melhores)")
        print(f"   Kafka é mais rápido em {wins['kafka']} de {len(metrics_to_compare)} métricas")
    else:
        print(f"🤝 EMPATE: Ambos empataram com {wins['rabbitmq']} métricas cada")
    
    # Diferença percentual na latência média
    avg_diff_percent = ((kafka_metrics['avg'] - rabbitmq_metrics['avg']) / 
                        rabbitmq_metrics['avg'] * 100)
    
    print(f"\n💡 INSIGHT PRINCIPAL:")
    if abs(avg_diff_percent) < 5:
        print(f"   As latências são MUITO SIMILARES (diferença de {abs(avg_diff_percent):.1f}%)")
        print(f"   Ambos os brokers têm performance comparável para este caso de uso.")
    elif avg_diff_percent > 0:
        print(f"   RabbitMQ é {abs(avg_diff_percent):.1f}% mais rápido que Kafka em média")
    else:
        print(f"   Kafka é {abs(avg_diff_percent):.1f}% mais rápido que RabbitMQ em média")
    
    # Análise de consistência
    rabbitmq_consistency = (rabbitmq_metrics['std'] / rabbitmq_metrics['avg']) * 100
    kafka_consistency = (kafka_metrics['std'] / kafka_metrics['avg']) * 100
    
    print(f"\n📈 ANÁLISE DE CONSISTÊNCIA:")
    print(f"   RabbitMQ - Coeficiente de Variação: {rabbitmq_consistency:.2f}%")
    print(f"   Kafka    - Coeficiente de Variação: {kafka_consistency:.2f}%")
    
    if rabbitmq_consistency < kafka_consistency:
        print(f"   🎯 RabbitMQ tem latência mais consistente (menor variação)")
    else:
        print(f"   🎯 Kafka tem latência mais consistente (menor variação)")
    
    print("\n" + "=" * 80)
    
    # Salvar comparação em arquivo
    with open('comparison_results5.txt', 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("COMPARAÇÃO DE LATÊNCIA: RABBITMQ vs KAFKA\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("📖 METODOLOGIA:\n")
        f.write("-" * 80 + "\n")
        f.write("Ambos os testes medem LATÊNCIA END-TO-END, que inclui:\n")
        f.write("  • Tempo de criação e serialização da mensagem no producer\n")
        f.write("  • Tempo de transmissão pela rede (localhost)\n")
        f.write("  • Tempo de processamento e armazenamento no broker\n")
        f.write("  • Tempo de entrega ao consumer\n")
        f.write("  • EXCLUI desserialização JSON (medida separadamente)\n\n")
        f.write("Configurações do teste:\n")
        f.write("  • 100 mensagens enviadas\n")
        f.write("  • Mensagens persistentes (durabilidade)\n")
        f.write("  • Ambiente: localhost (sem latência de rede real)\n")
        f.write("  • Python com bibliotecas pika (RabbitMQ) e kafka-python (Kafka)\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"{'Métrica':<20} | {'RabbitMQ':>15} | {'Kafka':>15} | "
               f"{'Diferença':>15} | {'Vencedor':<15}\n")
        f.write("-" * 80 + "\n")
        
        for label, key in metrics_to_compare:
            rabbitmq_val = rabbitmq_metrics.get(key, 0)
            kafka_val = kafka_metrics.get(key, 0)
            diff = kafka_val - rabbitmq_val
            
            if rabbitmq_val < kafka_val:
                winner = "RabbitMQ"
            elif kafka_val < rabbitmq_val:
                winner = "Kafka"
            else:
                winner = "Empate"
            
            f.write(f"{label:<20} | {rabbitmq_val:>12.2f} ms | {kafka_val:>12.2f} ms | "
                   f"{diff:>+12.2f} ms | {winner:<15}\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("RESUMO:\n")
        f.write("-" * 80 + "\n")
        
        if wins['rabbitmq'] > wins['kafka']:
            f.write(f"VENCEDOR: RabbitMQ ({wins['rabbitmq']}/{len(metrics_to_compare)} métricas)\n")
        elif wins['kafka'] > wins['rabbitmq']:
            f.write(f"VENCEDOR: Kafka ({wins['kafka']}/{len(metrics_to_compare)} métricas)\n")
        else:
            f.write(f"EMPATE: {wins['rabbitmq']}/{len(metrics_to_compare)} métricas cada\n")
        
        f.write(f"\nDiferença percentual na latência média: {avg_diff_percent:+.2f}%\n")
        f.write(f"RabbitMQ - Coeficiente de Variação: {rabbitmq_consistency:.2f}%\n")
        f.write(f"Kafka    - Coeficiente de Variação: {kafka_consistency:.2f}%\n")
        f.write("=" * 80 + "\n")
    
    print("\n💾 Comparação salva em 'comparison_results5.txt'")

if __name__ == "__main__":
    compare_brokers()
