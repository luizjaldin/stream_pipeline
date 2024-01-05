from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import datetime
import os

class StreamingPipeline:

    def __init__(self):
        self.kafka_bootstrap_servers = 'localhost:9092'
        self.kafka_topic = 'topic_streaming_data'
        self.output_path = os.getcwd().split('stream_pipeline')[0] + "stream_pipeline/output/topic_streaming_data/data_eletronicos_kpi"
        self.checkpoint_path = os.getcwd().split('stream_pipeline')[0] + "stream_pipeline/checkpoint_path/topic_streaming_data/data_eletronicos_kpi"

    def get_spark_session(self):
        
        os.environ["JAVA_HOME"] = "/usr/local/opt/openjdk@11"

        spark = SparkSession.builder \
                .config("spark.driver.host", "localhost") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
                .appName("StreamingPipeline") \
                .master("local[2]") \
                .getOrCreate()
        return spark

    def read_kafka(self, spark):
        streaming_df = spark.readStream \
                            .format("kafka") \
                            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                            .option("subscribe", self.kafka_topic) \
                            .option("startingOffsets", "earliest") \
                            .option("failOnDataLoss", "false") \
                            .load()
        
        json_schema = StructType([
                        StructField('vendedor', StructType([
                            StructField('nome', StringType()),
                            StructField('codigo', StringType()),
                            StructField('filial', StringType())
                        ])),
                        StructField('filial', StructType([
                            StructField('nome', StringType()),
                            StructField('estado', StringType()),
                            StructField('cidade', StringType())
                        ])),
                        StructField('produto', StructType([
                            StructField('codigo', StringType()),
                            StructField('nome', StringType()),
                            StructField('preço', DoubleType()),
                            StructField('categoria', StringType()),
                            StructField('cor', StringType())
                        ])),
                        StructField('quantidade', IntegerType()),
                        StructField('ordem_id', IntegerType()),
                        StructField('timestamp', StringType())
                    ])
        
        streaming_df = streaming_df.withColumn("value", F.decode(F.col("value"), 'UTF-8'))
        streaming_df = streaming_df.select(F.from_json(F.col('value').cast('string'), json_schema).alias('data'),'timestamp')
        streaming_df = streaming_df.withColumn("timestamp_venda", F.from_unixtime("data.timestamp"))

        filter_df = streaming_df.filter(F.col("data.produto.categoria") == 'Eletrônicos')\
                                .select(
                                    'data.produto.nome',
                                    'data.produto.preço',
                                    'data.quantidade',
                                    'data.filial.nome',
                                    'data.filial.cidade',
                                    'data.ordem_id',
                                    'timestamp'
                                )

        return filter_df

    def calcular_metricas(self, df):
        aggregated_df = df.withWatermark("timestamp", "10 minutes")\
                        .groupBy(F.window(F.col("timestamp"), "10 minutes"),"cidade").agg(F.sum( 
                                                                F.col("preço")*F.col("quantidade")
                                                                ).alias("total"),
                                                                F.sum("quantidade").alias("quantidade_vendida"),
                            )

        aggregated_df = aggregated_df.withColumn("valor_medio_de_venda", F.col("total")/F.col("quantidade_vendida"))
        return aggregated_df
    
    def save_data(self, df: DataFrame):
        df = df.withColumn("year", F.year(F.col("window.start")))
        df = df.withColumn("month",F.month(F.col("window.start")))
        df = df.withColumn("day", F.dayofmonth(F.col("window.start")))
        df = df.withColumn("hour", F.hour(F.col("window.start")))

        query = df.writeStream \
                            .outputMode("append") \
                            .format("parquet") \
                            .option("path", self.output_path) \
                            .option("checkpointLocation", self.checkpoint_path) \
                            .partitionBy("year", "month", "day", "hour") \
                            .trigger(processingTime='1 minute') \
                            .start()

    def run(self):
        spark = self.get_spark_session()
        df = self.calcular_metricas(self.read_kafka(spark))
        self.save_data(df)

        # Aguarde a conclusão do streaming (interrompa manualmente com Ctrl+C)
        try:
            spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            print("Parando o streaming manualmente.")
            spark.stop()
