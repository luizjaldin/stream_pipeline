from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
import random
import kafka.dados_de_negocio as bd
import datetime
import time
import json

class processador_de_vendas:
    def __init__(self):
        self.bootstrap_servers = 'localhost:9092'
        self.new_topic_name = 'topic_streaming_data'
        self.producer_config = {'bootstrap.servers': self.bootstrap_servers}

    def topic_exists(self):
        producer = Producer(self.producer_config)
        cluster_metadata = producer.list_topics(timeout=10)
        return self.new_topic_name in cluster_metadata.topics

    def create_topic(self):
        if self.topic_exists() == False:
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
            new_topic = NewTopic(self.new_topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics([new_topic])
        
    def send_message(self):
        producer = Producer(self.producer_config)

        for i in range(200):
            vendedor = random.choice(list(bd.vendedores.keys()))
            filial = bd.filiais.get(bd.vendedores.get(vendedor).get('filial'))
            produto = random.choice(list(bd.produtos.keys()))
            quantidade = random.choice([1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,3,3,4,5,6,7,8,9,10])

            evento_venda = {'vendedor' : bd.vendedores.get(vendedor),
                        'filial' : filial,
                        'produto' : bd.produtos.get(produto),
                        'quantidade' : quantidade,
                        'ordem_id':str(datetime.datetime.now().timestamp()).replace('.',''),
                        'timestamp': str(datetime.datetime.now().timestamp()),
                        }
            
            producer.produce(self.new_topic_name, value=json.dumps(evento_venda))
            producer.flush()
            time.sleep(random.randint(1,5))

    def run(self):
        self.create_topic()
        while True:
            self.send_message()