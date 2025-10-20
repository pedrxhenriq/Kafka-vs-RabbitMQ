Teste Básico de Enfileiramento

Metodologia de Execução
O teste de enfileiramento foi estruturado em duas etapas sequenciais e independentes. Primeiramente, o consumidor é inicializado e colocado em estado de espera ativa, configurado para processar mensagens assim que estas se tornem disponíveis no broker. Posteriormente, o produtor é executado para publicar 100 mensagens sequenciais no broker, cada uma contendo um payload JSON de aproximadamente 120 bytes com três campos principais: um identificador numérico único, um timestamp no formato ISO 8601.
A escolha de 100 mensagens proporciona uma amostra estatisticamente significativa para análise de desempenho, equilibrando precisão dos resultados com tempo de execução razoável. O payload de 120 bytes aproxima-se de cenários reais de comunicação entre microserviços, onde mensagens típicas transportam identificadores, timestamps e metadados sem grande volume de dados.
Medição de Métricas de Latência
A metodologia de medição de latência foi projetada para avaliar primordialmente o desempenho do broker na entrega de mensagens, minimizando a influência do processamento da aplicação cliente. O procedimento de medição segue o seguinte fluxo temporal:
T0 - Criação da Mensagem no Producer: O timestamp inicial é capturado imediatamente após a criação do objeto da mensagem, porém antes da serialização JSON. Este ponto temporal (T0) é armazenado no campo timestamp_ms da própria mensagem e serve como referência de origem para o cálculo de latência.
T1 - Serialização e Envio: A mensagem é serializada para o formato JSON e transmitida ao broker através da rede. Este processo inclui a conversão dos dados estruturados em bytes e o envio via protocolo TCP/IP.
T2 - Processamento no Broker: O broker recebe a mensagem, realiza as operações de persistência em disco (quando configurado para durabilidade), gerencia o enfileiramento interno e disponibiliza a mensagem para consumo. Este é o componente central de avaliação, representando a eficiência do sistema de mensageria.
T3 - Recebimento RAW no Consumer: O timestamp de recebimento (T3) é capturado imediatamente quando a mensagem bruta (em formato de bytes) chega ao consumidor, crucialmente antes de qualquer operação de desserialização. Este ponto de medição é fundamental para garantir que o tempo de processamento do JSON não seja incluído na métrica de latência.
T4 - Desserialização (Não Incluída na Latência Principal): Após a captura de T3, a mensagem é desserializada do formato JSON para estrutura de dados Python. O tempo desta operação é registrado separadamente para análise, mas não compõe a latência end-to-end medida, uma vez que representa overhead do cliente e não do broker.
A latência end-to-end é calculada pela diferença entre o timestamp de recebimento no consumidor (T3) e o timestamp de criação no produtor (T0):
Latência = T3 - T0
Esta abordagem assegura que a métrica captura o tempo total desde a criação da mensagem até sua disponibilização ao consumidor, incluindo serialização, transmissão de rede, processamento do broker e entrega, mas excluindo o processamento de desserialização no lado do cliente. Embora não seja possível isolar completamente o desempenho interno do broker de forma externa (sem instrumentação do código-fonte), esta metodologia minimiza significativamente a influência de fatores externos, fornecendo uma medição representativa da latência do broker em condições reais de uso.
Aspectos Técnicos da Implementação
Para garantir consistência entre as comparações de RabbitMQ e Kafka, ambos os consumidores foram implementados com a mesma estratégia de captura temporal. No caso do RabbitMQ, o timestamp T3 é capturado no início da função callback, antes da chamada json.loads(). Para o Kafka, o parâmetro value_deserializer foi configurado como None, forçando a desserialização manual após a captura do timestamp, em contraste com a deserialização automática que ocorreria antes da disponibilização da mensagem ao código do consumidor.
Cálculo de Métricas Estatísticas
Após o processamento completo das 100 mensagens, são computadas as seguintes métricas estatísticas sobre o conjunto de latências individuais:

Latência Média: Média aritmética de todas as latências, representando o comportamento típico esperado do sistema
Latência Mínima: Menor latência observada, indicando o melhor desempenho possível sob as condições do teste
Latência Máxima: Maior latência observada, revelando picos de latência que podem impactar aplicações sensíveis a tempo
Mediana (P50): Valor central da distribuição, menos sensível a outliers que a média
Desvio Padrão: Medida de dispersão dos valores, indicando a consistência do desempenho
Percentis (P90, P95, P99): Valores que delimitam os 10%, 5% e 1% piores casos, respectivamente, fundamentais para análise de latências de cauda (tail latency)

Todas as métricas são registradas em arquivo de texto estruturado, incluindo tanto o resumo estatístico quanto o conjunto completo de dados brutos (todos os timestamps T0, T3 e T4 de cada mensagem individual), permitindo análises posteriores e validação dos resultados. Esta documentação detalhada dos dados brutos possibilita a reprodutibilidade do experimento e facilita investigações adicionais sobre comportamentos anômalos ou padrões específicos observados durante a execução dos testes.