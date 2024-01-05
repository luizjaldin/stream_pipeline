# Iniciando o Kafka Localmente

Para utilizar o Kafka localmente, siga os passos abaixo:

1. **Compilar o Kafka (Somente na Primeira Vez):**
   - Abra o terminal na pasta `kafka-3.6.1-src` e execute o comando:
     ```bash
     ./gradlew jar -PscalaVersion=2.13.10
     ```
   
2. **Iniciar o Zookeeper:**
   - Ainda no mesmo terminal, execute o comando:
     ```bash
     bin/zookeeper-server-start.sh config/zookeeper.properties
     ```

3. **Iniciar o Kafka Broker:**
   - Abra um novo terminal na pasta `kafka-3.6.1-src` e execute o comando:
     ```bash
     bin/kafka-server-start.sh config/server.properties
     ```

4. **Verificar Mensagens Enviadas:**
   - Após executar o código gerador de mensagens, você pode visualizar as mensagens enviadas usando o seguinte comando:
     ```bash
     bin/kafka-console-consumer.sh --topic topic_streaming_data --bootstrap-server localhost:9092 --from-beginning
     ```

Isso permitirá que você inicie o Kafka localmente e monitore as mensagens no tópico `topic_streaming_data`. Certifique-se de ter o Kafka compilado (passo 1) antes de iniciar o Zookeeper e o Kafka Broker.

 
