O teste de throughput avalia a capacidade de processamento contínuo sob alto volume de mensagens, comparando o desempenho bruto de Apache Kafka e RabbitMQ. O experimento consiste no envio sequencial de 100.000 mensagens JSON (~100 bytes cada), processadas por um produtor e um consumidor em cada broker.
A métrica principal é a taxa de processamento (mensagens/segundo), calculada pela razão entre mensagens processadas e tempo total. O teste possui duas fases: publicação (producer) e consumo (consumer), permitindo análise separada de escrita e leitura.

Análise Arquitetural
Kafka: Otimização para Alto Volume
Principais características que impactam throughput:

Log append-only: Gravação sequencial em disco (mais rápida que I/O aleatório)
Batching agressivo: Agrupamento automático de mensagens reduz overhead de rede
Zero-copy: Transferência direta kernel→socket sem passar por user space
Polling em lote: Consumer busca até 500 mensagens por vez

Resultado: Superior capacidade de ingestão e leitura sequencial, ideal para alto volume sustentado.
RabbitMQ: Flexibilidade com Trade-offs
Principais características que impactam throughput:

Fila tradicional: Mensagens removidas após ACK, gerenciamento de estado por mensagem
Protocolo AMQP: Overhead mais pesado que protocolo binário do Kafka
Confirmações granulares: ACK individual adiciona round-trips de rede
Persistência transacional: Garantias ACID com trade-off de velocidade

Resultado: Throughput adequado para casos empresariais, priorizando flexibilidade e garantias de entrega.

Casos de Uso Ideais
Kafka
Alta ingestão: Logs centralizados, métricas, IoT (milhões de eventos/s)
Stream processing: ETL em tempo real, agregações contínuas
Event sourcing: Sistemas que precisam replay e auditoria completa

RabbitMQ
Processamento transacional: Pagamentos, pedidos com garantias fortes
Roteamento complexo: Notificações segmentadas, mensagens com prioridades
Work queues: Jobs assíncronos, tarefas de background, filas de retry

Conclusão
Kafka demonstrou throughput superior através de batching, I/O sequencial e arquitetura otimizada para volume. Ideal para cenários de alta taxa de dados.
RabbitMQ apresentou throughput adequado com overhead aceitável em troca de flexibilidade de roteamento e garantias transacionais. Ideal para processamento tradicional de mensagens.
A escolha deve considerar não apenas throughput, mas também requisitos de ordenação, persistência, reprocessamento e complexidade de roteamento.