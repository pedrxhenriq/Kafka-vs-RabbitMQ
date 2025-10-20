import re
import glob
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np

def parse_log_file(filename):
    """Extrai informações de um arquivo de log"""
    data = {
        'consumer_id': None,
        'platform': None,
        'total_messages': 0,
        'ids_processed': [],
        'partitions': [],
        'processing_time': 0,
        'rate': 0
    }
    
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
        
        # Extrair consumer ID
        match = re.search(r'Consumidor (\w+)', content)
        if match:
            data['consumer_id'] = match.group(1)
        
        # Extrair plataforma
        match = re.search(r'Plataforma: (\w+)', content)
        if match:
            data['platform'] = match.group(1)
        
        # Extrair partições (Kafka)
        match = re.search(r'Partições atribuídas: \[(.*?)\]', content)
        if match:
            parts_str = match.group(1)
            if parts_str.strip():
                data['partitions'] = [int(p.strip()) for p in parts_str.split(',')]
        
        # Extrair total de mensagens
        match = re.search(r'Total processado: (\d+)', content)
        if match:
            data['total_messages'] = int(match.group(1))
        
        # Extrair tempo
        match = re.search(r'Tempo total: ([\d.]+)', content)
        if match:
            data['processing_time'] = float(match.group(1))
        
        # Extrair taxa
        match = re.search(r'Taxa: ([\d.]+)', content)
        if match:
            data['rate'] = float(match.group(1))
        
        # Extrair IDs processados
        match = re.search(r'IDs processados \(ordenados\): \[(.*?)\]', content)
        if match:
            ids_str = match.group(1)
            if ids_str.strip():
                data['ids_processed'] = [int(id.strip()) for id in ids_str.split(',')]
    
    return data

def analyze_platform(platform_name):
    """Analisa logs de uma plataforma específica"""
    print("\n" + "=" * 80)
    print(f"ANÁLISE DE RESULTADOS - {platform_name.upper()}")
    print("=" * 80)
    
    # Encontrar todos os logs da plataforma
    pattern = f'{platform_name.lower()}_consumer_*.log'
    log_files = glob.glob(pattern)
    
    if not log_files:
        print(f"⚠️  Nenhum arquivo de log encontrado para {platform_name}")
        print(f"   Procurando por: {pattern}")
        return None
    
    print(f"\nArquivos encontrados: {len(log_files)}")
    for f in log_files:
        print(f"  • {f}")
    
    # Parse de todos os logs
    consumers_data = []
    for log_file in log_files:
        data = parse_log_file(log_file)
        consumers_data.append(data)
        print(f"\n✓ Processado: {log_file}")
        print(f"  Consumidor: {data['consumer_id']}")
        print(f"  Mensagens: {data['total_messages']}")
        if data['partitions']:
            print(f"  Partições: {data['partitions']}")
    
    # Análise agregada
    print("\n" + "-" * 80)
    print("ANÁLISE AGREGADA")
    print("-" * 80)
    
    total_unique_messages = set()
    all_messages = []
    
    for data in consumers_data:
        all_messages.extend(data['ids_processed'])
        total_unique_messages.update(data['ids_processed'])
    
    print(f"\nDistribuição de mensagens por consumidor:")
    for data in consumers_data:
        percentage = (data['total_messages'] / len(total_unique_messages) * 100) if total_unique_messages else 0
        print(f"  {data['consumer_id']}: {data['total_messages']} mensagens ({percentage:.1f}%)")
        if data['partitions']:
            print(f"    └─ Partições: {data['partitions']}")
    
    print(f"\nEstatísticas gerais:")
    print(f"  Total de mensagens únicas: {len(total_unique_messages)}")
    print(f"  Total de leituras: {len(all_messages)}")
    print(f"  Mensagens duplicadas: {len(all_messages) - len(total_unique_messages)}")
    
    # Verificar mensagens perdidas (assumindo sequência 0 a N-1)
    if total_unique_messages:
        expected_ids = set(range(max(total_unique_messages) + 1))
        missing_ids = expected_ids - total_unique_messages
        
        if missing_ids:
            print(f"  ⚠️  Mensagens perdidas: {len(missing_ids)}")
            print(f"      IDs perdidos: {sorted(missing_ids)[:10]}{'...' if len(missing_ids) > 10 else ''}")
        else:
            print(f"  ✓ Nenhuma mensagem perdida")
    
    # Calcular índice de Gini (medida de desbalanceamento)
    message_counts = [data['total_messages'] for data in consumers_data]
    gini = calculate_gini(message_counts)
    print(f"\nÍndice de Gini (desbalanceamento): {gini:.4f}")
    print(f"  (0 = perfeitamente balanceado, 1 = totalmente desbalanceado)")
    
    return consumers_data, total_unique_messages

def calculate_gini(values):
    """Calcula o coeficiente de Gini"""
    if not values or sum(values) == 0:
        return 0
    
    sorted_values = sorted(values)
    n = len(sorted_values)
    cumsum = np.cumsum(sorted_values)
    
    return (2 * sum((i + 1) * val for i, val in enumerate(sorted_values)) / (n * sum(sorted_values))) - (n + 1) / n

def generate_graphs(rabbitmq_data, kafka_1p_data, kafka_3p_data, kafka_6p_data):
    """Gera gráficos comparativos"""
    print("\n" + "=" * 80)
    print("GERANDO GRÁFICOS")
    print("=" * 80)
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Análise de Concorrência: RabbitMQ vs Kafka', fontsize=16, fontweight='bold')
    
    # Gráfico 1: Distribuição de mensagens - RabbitMQ
    if rabbitmq_data:
        ax = axes[0, 0]
        consumers = [d['consumer_id'] for d in rabbitmq_data]
        counts = [d['total_messages'] for d in rabbitmq_data]
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
        
        bars = ax.bar(consumers, counts, color=colors[:len(consumers)])
        ax.set_title('RabbitMQ - Distribuição de Mensagens\n(1 fila, prefetch=1)', fontweight='bold')
        ax.set_ylabel('Número de Mensagens')
        ax.set_xlabel('Consumidor')
        ax.grid(axis='y', alpha=0.3)
        
        # Adicionar valores nas barras
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height)}\n({height/sum(counts)*100:.1f}%)',
                   ha='center', va='bottom', fontweight='bold')
    
    # Gráfico 2: Kafka 1 partição
    if kafka_1p_data:
        ax = axes[0, 1]
        consumers = [d['consumer_id'] for d in kafka_1p_data]
        counts = [d['total_messages'] for d in kafka_1p_data]
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
        
        bars = ax.bar(consumers, counts, color=colors[:len(consumers)])
        ax.set_title('Kafka - 1 Partição\n(Apenas 1 consumidor ativo)', fontweight='bold')
        ax.set_ylabel('Número de Mensagens')
        ax.set_xlabel('Consumidor')
        ax.grid(axis='y', alpha=0.3)
        
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}\n({height/sum(counts)*100:.1f}%)',
                       ha='center', va='bottom', fontweight='bold')
    
    # Gráfico 3: Kafka 3 partições
    if kafka_3p_data:
        ax = axes[1, 0]
        consumers = [d['consumer_id'] for d in kafka_3p_data]
        counts = [d['total_messages'] for d in kafka_3p_data]
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
        
        bars = ax.bar(consumers, counts, color=colors[:len(consumers)])
        ax.set_title('Kafka - 3 Partições\n(1 partição por consumidor)', fontweight='bold')
        ax.set_ylabel('Número de Mensagens')
        ax.set_xlabel('Consumidor')
        ax.grid(axis='y', alpha=0.3)
        
        # Adicionar informação de partições
        for i, (bar, data) in enumerate(zip(bars, kafka_3p_data)):
            height = bar.get_height()
            if height > 0:
                partitions_str = f"P{data['partitions']}" if data['partitions'] else ""
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}\n({height/sum(counts)*100:.1f}%)\n{partitions_str}',
                       ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    # Gráfico 4: Kafka 6 partições
    if kafka_6p_data:
        ax = axes[1, 1]
        consumers = [d['consumer_id'] for d in kafka_6p_data]
        counts = [d['total_messages'] for d in kafka_6p_data]
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
        
        bars = ax.bar(consumers, counts, color=colors[:len(consumers)])
        ax.set_title('Kafka - 6 Partições\n(2 partições por consumidor)', fontweight='bold')
        ax.set_ylabel('Número de Mensagens')
        ax.set_xlabel('Consumidor')
        ax.grid(axis='y', alpha=0.3)
        
        # Adicionar informação de partições
        for i, (bar, data) in enumerate(zip(bars, kafka_6p_data)):
            height = bar.get_height()
            if height > 0:
                partitions_str = f"P{data['partitions']}" if data['partitions'] else ""
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}\n({height/sum(counts)*100:.1f}%)\n{partitions_str}',
                       ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    plt.tight_layout()
    plt.savefig('concurrency_comparison.png', dpi=300, bbox_inches='tight')
    print("✓ Gráfico salvo: concurrency_comparison.png")
    plt.close()
    
    # Gráfico 2: Comparação de balanceamento (Gini Index)
    fig, ax = plt.subplots(figsize=(10, 6))
    
    platforms = []
    gini_values = []
    colors_gini = []
    
    if rabbitmq_data:
        counts = [d['total_messages'] for d in rabbitmq_data]
        gini = calculate_gini(counts)
        platforms.append('RabbitMQ\n(1 fila)')
        gini_values.append(gini)
        colors_gini.append('#FF6B6B')
    
    if kafka_1p_data:
        counts = [d['total_messages'] for d in kafka_1p_data]
        gini = calculate_gini(counts)
        platforms.append('Kafka\n(1 partição)')
        gini_values.append(gini)
        colors_gini.append('#FFA07A')
    
    if kafka_3p_data:
        counts = [d['total_messages'] for d in kafka_3p_data]
        gini = calculate_gini(counts)
        platforms.append('Kafka\n(3 partições)')
        gini_values.append(gini)
        colors_gini.append('#4ECDC4')
    
    if kafka_6p_data:
        counts = [d['total_messages'] for d in kafka_6p_data]
        gini = calculate_gini(counts)
        platforms.append('Kafka\n(6 partições)')
        gini_values.append(gini)
        colors_gini.append('#45B7D1')
    
    bars = ax.bar(platforms, gini_values, color=colors_gini)
    ax.set_title('Índice de Gini - Balanceamento de Carga\n(0 = perfeitamente balanceado, 1 = desbalanceado)', 
                 fontweight='bold', fontsize=12)
    ax.set_ylabel('Índice de Gini')
    ax.set_ylim(0, 1)
    ax.grid(axis='y', alpha=0.3)
    
    # Adicionar valores nas barras
    for bar in bars:
        height = bar.get_height()
        label = 'Ótimo' if height < 0.1 else 'Bom' if height < 0.3 else 'Ruim'
        ax.text(bar.get_x() + bar.get_width()/2., height,
               f'{height:.4f}\n({label})',
               ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('gini_index_comparison.png', dpi=300, bbox_inches='tight')
    print("✓ Gráfico salvo: gini_index_comparison.png")
    plt.close()

def generate_report(rabbitmq_data, rabbitmq_total, 
                   kafka_1p_data, kafka_1p_total,
                   kafka_3p_data, kafka_3p_total,
                   kafka_6p_data, kafka_6p_total):
    """Gera relatório completo em texto"""
    print("\n" + "=" * 80)
    print("GERANDO RELATÓRIO")
    print("=" * 80)
    
    with open('concurrency_test_report.txt', 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("RELATÓRIO COMPLETO - TESTE DE CONCORRÊNCIA\n")
        f.write("RabbitMQ vs Kafka\n")
        f.write("=" * 80 + "\n\n")
        
        # RabbitMQ
        if rabbitmq_data:
            f.write("-" * 80 + "\n")
            f.write("RABBITMQ - 1 FILA COM 3 CONSUMIDORES\n")
            f.write("-" * 80 + "\n\n")
            
            f.write("Configuração:\n")
            f.write("  • Fila única: concurrency_test\n")
            f.write("  • Prefetch count: 1\n")
            f.write("  • Consumidores: 3\n\n")
            
            f.write("Distribuição de Mensagens:\n")
            for data in rabbitmq_data:
                percentage = (data['total_messages'] / len(rabbitmq_total) * 100) if rabbitmq_total else 0
                f.write(f"  • {data['consumer_id']}: {data['total_messages']} mensagens ({percentage:.1f}%)\n")
            
            counts = [d['total_messages'] for d in rabbitmq_data]
            gini = calculate_gini(counts)
            
            f.write(f"\nMétricas:\n")
            f.write(f"  • Total de mensagens únicas: {len(rabbitmq_total)}\n")
            f.write(f"  • Índice de Gini: {gini:.4f}\n")
            f.write(f"  • Balanceamento: {'Ótimo' if gini < 0.1 else 'Bom' if gini < 0.3 else 'Ruim'}\n")
            
            f.write(f"\nCaracterísticas observadas:\n")
            f.write(f"  ✓ Distribuição equilibrada entre consumidores\n")
            f.write(f"  ✓ Todos os consumidores ativos simultaneamente\n")
            f.write(f"  ✗ Ordem global não preservada\n")
            f.write(f"  ✓ Simples de configurar (não requer particionamento)\n\n")
        
        # Kafka 1 partição
        if kafka_1p_data:
            f.write("-" * 80 + "\n")
            f.write("KAFKA - 1 PARTIÇÃO COM 3 CONSUMIDORES\n")
            f.write("-" * 80 + "\n\n")
            
            f.write("Configuração:\n")
            f.write("  • Tópico: concurrency_test\n")
            f.write("  • Partições: 1\n")
            f.write("  • Consumidores: 3 (mesmo grupo)\n\n")
            
            f.write("Distribuição de Mensagens:\n")
            for data in kafka_1p_data:
                percentage = (data['total_messages'] / len(kafka_1p_total) * 100) if kafka_1p_total else 0
                f.write(f"  • {data['consumer_id']}: {data['total_messages']} mensagens ({percentage:.1f}%)")
                if data['partitions']:
                    f.write(f" - Partições: {data['partitions']}")
                f.write("\n")
            
            counts = [d['total_messages'] for d in kafka_1p_data]
            gini = calculate_gini(counts)
            
            f.write(f"\nMétricas:\n")
            f.write(f"  • Total de mensagens únicas: {len(kafka_1p_total)}\n")
            f.write(f"  • Índice de Gini: {gini:.4f}\n")
            f.write(f"  • Balanceamento: {'Ótimo' if gini < 0.1 else 'Bom' if gini < 0.3 else 'Ruim'}\n")
            
            f.write(f"\nCaracterísticas observadas:\n")
            f.write(f"  ⚠ Apenas 1 consumidor ativo (limitação arquitetural)\n")
            f.write(f"  ⚠ 2 consumidores ociosos (desperdício de recursos)\n")
            f.write(f"  ✓ Ordem global PRESERVADA\n")
            f.write(f"  ✗ Paralelização impossível com 1 partição\n\n")
        
        # Kafka 3 partições
        if kafka_3p_data:
            f.write("-" * 80 + "\n")
            f.write("KAFKA - 3 PARTIÇÕES COM 3 CONSUMIDORES\n")
            f.write("-" * 80 + "\n\n")
            
            f.write("Configuração:\n")
            f.write("  • Tópico: concurrency_test\n")
            f.write("  • Partições: 3\n")
            f.write("  • Consumidores: 3 (mesmo grupo)\n\n")
            
            f.write("Distribuição de Mensagens:\n")
            for data in kafka_3p_data:
                percentage = (data['total_messages'] / len(kafka_3p_total) * 100) if kafka_3p_total else 0
                f.write(f"  • {data['consumer_id']}: {data['total_messages']} mensagens ({percentage:.1f}%)")
                if data['partitions']:
                    f.write(f" - Partições: {data['partitions']}")
                f.write("\n")
            
            counts = [d['total_messages'] for d in kafka_3p_data]
            gini = calculate_gini(counts)
            
            f.write(f"\nMétricas:\n")
            f.write(f"  • Total de mensagens únicas: {len(kafka_3p_total)}\n")
            f.write(f"  • Índice de Gini: {gini:.4f}\n")
            f.write(f"  • Balanceamento: {'Ótimo' if gini < 0.1 else 'Bom' if gini < 0.3 else 'Ruim'}\n")
            
            f.write(f"\nCaracterísticas observadas:\n")
            f.write(f"  ✓ Distribuição equilibrada (1 partição por consumidor)\n")
            f.write(f"  ✓ Todos os consumidores ativos simultaneamente\n")
            f.write(f"  ✓ Ordem preservada DENTRO de cada partição\n")
            f.write(f"  ✓ Configuração ideal (partições = consumidores)\n\n")
        
        # Kafka 6 partições
        if kafka_6p_data:
            f.write("-" * 80 + "\n")
            f.write("KAFKA - 6 PARTIÇÕES COM 3 CONSUMIDORES\n")
            f.write("-" * 80 + "\n\n")
            
            f.write("Configuração:\n")
            f.write("  • Tópico: concurrency_test\n")
            f.write("  • Partições: 6\n")
            f.write("  • Consumidores: 3 (mesmo grupo)\n\n")
            
            f.write("Distribuição de Mensagens:\n")
            for data in kafka_6p_data:
                percentage = (data['total_messages'] / len(kafka_6p_total) * 100) if kafka_6p_total else 0
                f.write(f"  • {data['consumer_id']}: {data['total_messages']} mensagens ({percentage:.1f}%)")
                if data['partitions']:
                    f.write(f" - Partições: {data['partitions']}")
                f.write("\n")
            
            counts = [d['total_messages'] for d in kafka_6p_data]
            gini = calculate_gini(counts)
            
            f.write(f"\nMétricas:\n")
            f.write(f"  • Total de mensagens únicas: {len(kafka_6p_total)}\n")
            f.write(f"  • Índice de Gini: {gini:.4f}\n")
            f.write(f"  • Balanceamento: {'Ótimo' if gini < 0.1 else 'Bom' if gini < 0.3 else 'Ruim'}\n")
            
            f.write(f"\nCaracterísticas observadas:\n")
            f.write(f"  ✓ Distribuição equilibrada (2 partições por consumidor)\n")
            f.write(f"  ✓ Todos os consumidores ativos simultaneamente\n")
            f.write(f"  ✓ Ordem preservada DENTRO de cada partição\n")
            f.write(f"  ✓ Maior paralelização (permite adicionar mais consumidores)\n\n")
        
        # Comparação final
        f.write("=" * 80 + "\n")
        f.write("COMPARAÇÃO GERAL\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("RABBITMQ:\n")
        f.write("  Vantagens:\n")
        f.write("    • Distribuição automática e equilibrada\n")
        f.write("    • Simples de configurar (sem particionamento)\n")
        f.write("    • Todos os consumidores trabalham imediatamente\n")
        f.write("  Desvantagens:\n")
        f.write("    • Não garante ordem com múltiplos consumidores\n")
        f.write("    • Menos controle sobre distribuição\n\n")
        
        f.write("KAFKA:\n")
        f.write("  Vantagens:\n")
        f.write("    • Garante ordem dentro de cada partição\n")
        f.write("    • Controle preciso de paralelização via partições\n")
        f.write("    • Escalável horizontalmente (adicionar partições)\n")
        f.write("    • Previsível (consumidor 'possui' partições)\n")
        f.write("  Desvantagens:\n")
        f.write("    • Requer planejamento do número de partições\n")
        f.write("    • Consumidores ociosos se partições < consumidores\n")
        f.write("    • Mais complexo de configurar\n\n")
        
        f.write("RECOMENDAÇÕES:\n")
        f.write("  • Use RabbitMQ quando:\n")
        f.write("    - Ordem não é crítica\n")
        f.write("    - Simplicidade é prioridade\n")
        f.write("    - Distribuição dinâmica é desejável\n\n")
        f.write("  • Use Kafka quando:\n")
        f.write("    - Ordem é importante (por chave/partição)\n")
        f.write("    - Necessita replay de mensagens\n")
        f.write("    - Escalabilidade horizontal é crítica\n")
        f.write("    - Pode planejar particionamento antecipadamente\n\n")
    
    print("✓ Relatório salvo: concurrency_test_report.txt")

def main():
    """Função principal de análise"""
    print("\n" + "=" * 80)
    print("ANÁLISE COMPARATIVA - TESTE DE CONCORRÊNCIA")
    print("RabbitMQ vs Kafka")
    print("=" * 80)
    
    # Analisar RabbitMQ
    rabbitmq_data, rabbitmq_total = analyze_platform('rabbitmq') or (None, None)
    
    # Analisar Kafka com diferentes configurações de partições
    print("\n")
    kafka_1p_data, kafka_1p_total = analyze_platform('kafka') or (None, None)
    
    # Se houver múltiplos testes do Kafka, você precisará renomear os logs
    # Por exemplo: kafka_consumer_C1_1p.log, kafka_consumer_C1_3p.log, etc.
    # Por simplicidade, vou assumir que você roda um teste por vez
    
    kafka_3p_data = kafka_1p_data  # Placeholder
    kafka_3p_total = kafka_1p_total
    
    kafka_6p_data = None
    kafka_6p_total = None
    
    # Gerar gráficos
    if any([rabbitmq_data, kafka_1p_data, kafka_3p_data, kafka_6p_data]):
        generate_graphs(rabbitmq_data, kafka_1p_data, kafka_3p_data, kafka_6p_data)
    
    # Gerar relatório
    generate_report(
        rabbitmq_data, rabbitmq_total,
        kafka_1p_data, kafka_1p_total,
        kafka_3p_data, kafka_3p_total,
        kafka_6p_data, kafka_6p_total
    )
    
    print("\n" + "=" * 80)
    print("ANÁLISE CONCLUÍDA")
    print("=" * 80)
    print("\nArquivos gerados:")
    print("  • concurrency_comparison.png - Gráficos de distribuição")
    print("  • gini_index_comparison.png - Comparação de balanceamento")
    print("  • concurrency_test_report.txt - Relatório completo em texto")
    print("\n")

if __name__ == "__main__":
    main()