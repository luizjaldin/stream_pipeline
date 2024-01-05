# Stream Pipeline

O objetivo deste repositório é criar um pipeline de streaming usando Kafka e Spark Streaming para processar e salvar mensagens em formato Parquet.

## Configuração do Ambiente

1. **Kafka e Zookeeper:**
   - Siga as instruções no README da pasta `kafka` para iniciar o Kafka e o Zookeeper.

2. **Gerar Mensagens de Exemplo:**
   - Execute o notebook `gerador_de_mensagens_kafka.ipynb` na pasta `kafka` para criar um tópico e enviar 200 mensagens simulando eventos de compra.

3. **Processamento em Tempo Real:**
   - Utilize o notebook `stream_data.ipynb` na pasta `pipeline` para ler as mensagens do Kafka, realizar transformações e salvar os dados como Parquet.
   - Este notebook também oferece a capacidade de ler os arquivos salvos e imprimir os resultados.

## Execução dos Scripts em Ambiente de Deploy

Para simular um ambiente de deploy, foram criados 4 scripts:

1. **gerador_mensagens_kafka.py:**
   - Envia continuamente mensagens ao tópico Kafka. Pode ser executado independentemente.

2. **run_kafka.py:**
   - Instancia o gerador de mensagens e o mantém em execução contínua.

3. **stream_data.py:**
   - Realiza o processamento de leitura, filtro e agregação das mensagens do Kafka, mas interrompe antes de salvar o arquivo Parquet.

4. **run_pipeline.py:**
   - Instancia o `stream_data.py` e o mantém em execução contínua.

**Instruções:**
- Certifique-se de que o Kafka e o Zookeeper estejam em execução.
- Execute `run_kafka.py` em um terminal e `run_pipeline.py` em outro para criar um fluxo constante de mensagens e processamento.

## Observações
- Os notebooks fornecem uma maneira interativa de experimentar com o pipeline.
- Os scripts de deploy simulam a execução contínua do pipeline em um ambiente de produção.

**Nota:** Adapte os caminhos e configurações conforme necessário para o seu ambiente.
