## Iniciando o Kafka localmente

Abra o terminal e dentro da pasta kafka-3.6.1-src execute os comandos abaixo
 - ./gradlew jar -PscalaVersion=2.13.10 (Essa linha é necessária apenas na primeira vez)
 - bin/zookeeper-server-start.sh config/zookeeper.properties
Abra um novo terminal e vá até a mesma pasta e execute:
 - bin/kafka-server-start.sh config/server.properties
 
 Após executar o código gerador de menssagens é possível ver as menssagens enviadas pelo comando bin/kafka-console-consumer.sh --topic topic_streaming_data --bootstrap-server localhost:9092 --from-beginning
 
