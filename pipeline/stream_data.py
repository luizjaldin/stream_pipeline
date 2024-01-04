from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, window
from pyspark.sql.streaming import Trigger

# Criar uma sessão Spark
spark = SparkSession.builder \
    .appName("StreamingPipeline") \
    .master("local[2]") \
    .getOrCreate()

# Definir a fonte de dados (pode ser alterado para a fonte desejada)
data_source = "path/to/your/json/data"

# Ler os dados de streaming a partir da fonte
streaming_df = spark.readStream \
    .format("json") \
    .option("maxFilesPerTrigger", 1) \
    .json(data_source)

# Bônus 1: Adicionar uma etapa de filtro
filtered_df = streaming_df.filter(col("your_column") > 10)

# Bônus 2: Adicionar uma etapa de agregação
aggregated_df = filtered_df.groupBy("your_grouping_column").agg(sum("your_aggregation_column").alias("total"))

# Bônus 3: Utilizar a função Window para agregar dados em janelas de tempo
windowed_df = filtered_df \
    .groupBy(window(col("timestamp_column"), "10 minutes"), col("your_grouping_column")) \
    .agg(sum("your_aggregation_column").alias("total"))

# Definir a saída (pode ser alterado para o formato desejado, como Parquet)
output_path = "path/to/your/output"

# Iniciar o streaming e escrever os resultados
query = windowed_df.writeStream \
    .outputMode("complete") \
    .format("parquet") \
    .option("path", output_path) \
    .trigger(Trigger.ProcessingTime("1 minute")) \
    .start()

# Aguardar a conclusão do streaming
query.awaitTermination()
