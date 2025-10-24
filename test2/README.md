Teste de Concorrência com Múltiplos Consumidores

O teste de concorrência foi estruturado para demonstrar as diferenças arquitetônicas fundamentais entre RabbitMQ e Kafka no tratamento de múltiplos consumidores. O experimento consiste na publicação de 10.000 mensagens numeradas sequencialmente (ID 0 a 9.999) processadas simultaneamente por 3 consumidores concorrentes.
Ambos os sistemas foram configurados com 3 consumidores conectados antes do início da publicação das mensagens. No RabbitMQ, os consumidores fazem parte de um consumer group implícito da fila. No Kafka, foram criadas 3 partições no tópico, com cada consumidor atribuído manualmente a uma partição específica através de atribuição explícita (Consumer 1 → Partição 0, Consumer 2 → Partição 1, Consumer 3 → Partição 2).

RabbitMQ: Round-Robin por Mensagem
O RabbitMQ implementa distribuição round-robin no nível da mensagem individual. O broker rotaciona automaticamente entre os consumidores disponíveis, entregando cada mensagem ao próximo consumidor na sequência cíclica. Este comportamento resulta em:
- Distribuição intercalada: Consumer 1 recebe mensagens 0, 3, 8, 11, 17, 20, 24..., sem padrão previsível (Utilizando como base o consumer 1)
- Perda de ordem global: As mensagens não mantêm sua sequência original quando distribuídas entre consumidores
- Balanceamento dinâmico: O broker decide em tempo real qual consumidor receberá cada mensagem

Kafka: Particionamento Fixo
O Kafka adota uma arquitetura baseada em partições com atribuição fixa de consumidores. As mensagens são distribuídas entre partições durante a publicação (usando round-robin quando nenhuma chave é especificada), e cada consumidor fica responsável por partições específicas. Este comportamento resulta em:
- Distribuição previsível: Consumer 1 recebe exclusivamente mensagens 0, 3, 6, 9, 12, 15... (múltiplos de 3 da Partição 0) (Utilizando como base o consumer 1)
- Ordem garantida dentro da partição: As mensagens mantêm ordem sequencial através dos offsets (offset 0→1→2→3...)
- Vinculação estática: Cada consumidor processa todas as mensagens de suas partições atribuídas até que ocorra rebalanceamento

Distribuição de Carga Observada
RabbitMQ
Consumer 1: 3.324 mensagens (33,24%)
Consumer 2: 3.331 mensagens (33,31%)
Consumer 3: 3.345 mensagens (33,45%)
Distribuição uniforme com variação mínima (~0,21 ponto percentual entre o menor e o maior)

Kafka
Consumer 1 (Partição 0): 3.334 mensagens (33,34%)
Consumer 2 (Partição 1): 3.333 mensagens (33,33%)
Consumer 3 (Partição 2): 3.333 mensagens (33,33%)
Distribuição matemática perfeita através de partition = message_id % 3

Análise de Sequenciamento
Verificação de Ordem - RabbitMQ
A análise dos IDs processados pelo Consumer 1 do RabbitMQ revela quebra de sequência:
Sequência observada: 0 → 3 → 8 → 11 → 17 → 20 → 24 → 27 → 29 → 34 (Utilizando como base o consumer 1)
Gaps variáveis: diferença entre IDs consecutivos varia de 2 a 6 mensagens
Imprevisibilidade: impossível determinar qual consumidor processará cada mensagem específica

Verificação de Ordem - Kafka
A análise dos IDs e offsets processados pelo Consumer 1 do Kafka confirma ordenação estrita:
IDs processados: 0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30... (Utilizando como base o consumer 1)
Offsets correspondentes: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10...
Progressão matemática: ID sempre = offset × 3
Ordem garantida: dentro da partição, a sequência é absolutamente preservada

Implicações Práticas
Casos de Uso Ideais - RabbitMQ
Processamento de tarefas independentes: jobs, notificações, emails onde a ordem não importa
Work queues: distribuição de trabalho onde cada task é autocontida
Balanceamento dinâmico de carga: quando consumidores têm capacidade heterogênea

Casos de Uso Ideais - Kafka
Event sourcing: logs de eventos que devem manter ordem temporal
Stream processing: análise de séries temporais que dependem de sequência
Auditoria e compliance: sistemas onde rastreabilidade e ordem são requisitos legais
Transações distribuídas: operações que dependem de sequência causal

Comparação de Performance
Métrica                 Kafka           RabbitMQ            Diferença
Taxa de processamento   6.942 msgs/s    2.885 msgs/s        Kafka 2,4x mais rápido
Latência média          18,93 ms        293,72 ms           RabbitMQ 15,5x mais lento
Latência mínima         3,94 ms         0,75 ms             Similar
Latência máxima         41,53 ms        480,68 ms           RabbitMQ mais instável
Desvio padrão           7,97 ms         145,65 ms           Kafka mais consistente