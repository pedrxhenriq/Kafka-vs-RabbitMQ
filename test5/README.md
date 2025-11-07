Teste de Retenção e Reprocessamento de Mensagens
Metodologia de Execução
O teste avalia a capacidade de retenção e reprocessamento de mensagens comparando Apache Kafka e RabbitMQ. São publicadas 50 mensagens sequenciais com payload JSON contendo identificador, timestamp e conteúdo textual.
Fluxo do Teste
1. Publicação: 50 mensagens enviadas para o broker
Kafka: tópico retention_test (1 partição, 1 réplica)
RabbitMQ: fila retention_test (mensagens persistentes)
2. Primeira Leitura: Consumo completo das mensagens
Kafka: consumer group retention_group com auto-commit habilitado
RabbitMQ: consumo com basic_ack() para confirmar recebimento
3. Segunda Leitura: Tentativa de reprocessamento
Kafka: novo consumer group retention_group_reprocess lê do offset 0
RabbitMQ: tentativa de consumo na mesma fila

Coleta de Dados
Para cada mensagem são registrados:
ID: Identificador sequencial (0-49)
Timestamp: Horário de criação e leitura (ISO 8601)
Offset/Partition (Kafka): Posição na partição
Delivery Tag (RabbitMQ): Identificador de entrega

Dados salvos em kafka_retention_results.txt e rabbitmq_retention_results.txt.
Resultados Esperados
Kafka
Primeira leitura: 50 mensagens consumidas (offsets 0-49)
Segunda leitura: 50 mensagens reprocessadas (mesmos offsets)
Conclusão: ✅ Reprocessamento bem-sucedido - mensagens persistem após consumo

RabbitMQ
Primeira leitura: 50 mensagens consumidas e removidas após ACK
Segunda leitura: 0 mensagens (fila vazia)
Conclusão: ❌ Reprocessamento impossível - mensagens descartadas após ACK

Diferenças Arquiteturais
Kafka: Log distribuído persistente. Mensagens mantidas em disco independente do consumo. Controle via offsets permite leitura histórica.
RabbitMQ: Fila volátil tradicional. Mensagens removidas após confirmação. Modelo de entrega única sem histórico.
Aplicações Práticas
Kafka: Streaming de eventos, análise histórica, reprocessamento após falhas, múltiplos consumidores independentes.
RabbitMQ: Processamento transacional, task queues, notificações, workflows de processamento único.