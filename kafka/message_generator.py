from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
import business_data as bd

# Configurações do Kafka (substitua pelos valores do seu ambiente)
bootstrap_servers = 'your_bootstrap_servers'
new_topic_name = 'your_new_topic'

# Criar um tópico Kafka
admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

new_topic = NewTopic(new_topic_name, num_partitions=1, replication_factor=1)
admin_client.create_topics([new_topic])

# Enviar mensagens para o tópico Kafka
producer_config = {'bootstrap.servers': bootstrap_servers}

producer = Producer(producer_config)

bd.filial

# Enviar algumas mensagens de exemplo
for i in range(5):
    message_value = f'Mensagem de exemplo {i}'
    producer.produce(new_topic_name, value=message_value)
    producer.flush()

print(f'Mensagens enviadas para o tópico "{new_topic_name}"')
