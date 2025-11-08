Teste de Resiliência sob Sobrecarga - Detecção de Threshold
Metodologia de Execução
O teste avalia o limite operacional (threshold) de Apache Kafka e RabbitMQ sob sobrecarga progressiva de mensagens. O objetivo é identificar o ponto de saturação onde o sistema perde estabilidade no throughput e latência.
Fluxo do Teste
1. Preparação

Limpeza e recriação da fila/tópico
Inicialização do consumer com prefetch de 1000 mensagens
Aguardo de 5 segundos para estabilização

2. Execução em Fases Progressivas

Fase 0: 50.000 mensagens
Fases subsequentes: incremento de 50.000 mensagens por fase
Máximo: 20 fases (até 1.000.000 mensagens)
Envio: velocidade máxima (sem sleep entre mensagens)
Aguardo: 3 segundos após cada fase para processamento

3. Payload das Mensagens

Formato JSON (~70 bytes)
Campos: id, phase_id, timestamp_ms, data (string de 50 caracteres)
Configuração: mensagens não-persistentes (acks=1 no Kafka, delivery_mode=1 no RabbitMQ)

Critérios de Detecção de Threshold
O sistema considera threshold atingido quando qualquer condição é satisfeita:

Degradação de Latência: aumento >100% em relação à baseline (fase 0)
Perda de Mensagens: taxa de perda >10%
Queda de Throughput: redução >30% entre fases consecutivas

O teste continua por 2 fases adicionais após detecção do threshold para confirmar a degradação.
Coleta de Dados
Para cada fase são registrados:
Métricas de Envio

target_msgs: quantidade de mensagens planejadas
sent: mensagens efetivamente enviadas
actual_rate_msg_s: throughput real de envio (msg/s)
duration_s: tempo total da fase

Métricas de Recebimento

received: mensagens consumidas
lost: diferença entre enviadas e recebidas
loss_pct: percentual de perda

Métricas de Latência

avg_latency_ms: latência média end-to-end
p99_latency_ms: percentil 99 (tail latency)
max_latency_ms: pior caso observado
degradation_pct: aumento percentual em relação à baseline

Dados salvos em formato CSV para análise posterior.
Métricas Comparativas
Análise de Threshold
Volume de Saturação

Número de mensagens na fase onde o threshold foi detectado
Comparação direta: quantas vezes mais um sistema suporta que o outro

Throughput no Limite

Taxa de processamento (msg/s) na fase de threshold
Razão entre throughputs: indica quantas vezes mais rápido um sistema opera

Degradação de Latência

Aumento percentual da latência em relação à baseline
Comparação de estabilidade: qual sistema degrada mais rapidamente

Análise de Estabilidade
Consistência de Performance

Desvio padrão do throughput entre fases pré-threshold
Variação da latência média: sistemas estáveis mantêm baixa variação

Recuperação Pós-Saturação

Throughput nas fases após threshold
Capacidade de manter processamento mesmo degradado

Resultados Obtidos
Apache Kafka

Threshold: Fase 10 (550.000 mensagens)
Throughput no limite: 8.939 msg/s
Latência baseline: 96,9 ms
Latência no threshold: 241,7 ms (+149,5%)
Perda de mensagens: 0%
Throughput médio global: 9.592 msg/s

RabbitMQ

Threshold: Fase 2 (150.000 mensagens)
Throughput no limite: 2.045 msg/s
Latência baseline: 1,9 ms
Latência no threshold: 3,8 ms (+101,1%)
Perda de mensagens: 0%
Throughput médio global: 2.812 msg/s

Conclusões
Capacidade de Sobrecarga
Kafka demonstra resiliência superior:

Suporta 3,7x mais mensagens antes do colapso (550k vs 150k)
Mantém 4,4x maior throughput no ponto de saturação (8.939 vs 2.045 msg/s)
Throughput médio 3,4x superior durante todo o teste (9.592 vs 2.812 msg/s)

Perfil de Latência
RabbitMQ oferece menor latência em operação normal:

Latência baseline 51x menor (1,9 ms vs 96,9 ms)
Ideal para comunicação de baixa latência em volumes moderados

Kafka prioriza throughput sobre latência:

Latência inicial mais alta devido ao batching e replicação
Degradação mais acentuada sob saturação (+149,5% vs +101,1%)

Diferenças Arquiteturais
Kafka:

Arquitetura de log particionado com gravação sequencial em disco
Maior tolerância ao acúmulo de mensagens (backpressure)
Escalabilidade horizontal via partições

RabbitMQ:

Modelo de fila em memória com persistência opcional
Saturação rápida quando buffer de memória se esgota
Limitado pela capacidade de um único nó (sem clustering)

Aplicações Práticas
Use Kafka quando:

Alto throughput é crítico (>5.000 msg/s)
Sistema precisa lidar com picos extremos de carga
Reprocessamento histórico é necessário
Latência de ~100ms é aceitável

Use RabbitMQ quando:

Baixa latência é prioritária (<10ms)
Volumes são moderados (<100.000 msg/dia)
Simplicidade operacional é valorizada
Padrões complexos de roteamento são necessários