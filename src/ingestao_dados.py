from minio import Minio
from minio.error import S3Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType


from classes.conecta_minio import ConectaMinio
from classes.pyspark_minio import PysparkMinio


if __name__ == "__main__":

    minio = ConectaMinio(
        host= "minio", 
        porta= 9000, 
        access_key= 'minio_access_key', 
        secret_key= 'minio_secret_key'
    )
    minio.criar_bucket('gold')

    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/code/jars/aws-java-sdk-bundle-1.11.1026.jar, /code/jars/hadoop-aws-3.3.2.jar") \
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    esquema_tabela = StructType([
        StructField("id", IntegerType(), nullable=True),
        StructField("categoria", StringType(), nullable=True),
        StructField("data", TimestampType(), nullable=True),
        StructField("valor", DoubleType(), nullable=True)
    ])
    caminho_tabela_atual = '/code/arquivos/tabela_atual.csv'
    df_tabela_atual = spark.read.option('header', 'true').schema(esquema_tabela).option('inferSchema', 'true').csv(caminho_tabela_atual)
    caminho_tabela_historico = '/code/arquivos/tabela_historico.csv'
    df_tabela_historico = spark.read.option('header', 'true').schema(esquema_tabela).option('inferSchema', 'true').csv(caminho_tabela_historico)

    pyspark_minio = PysparkMinio(
        spark_session = spark,
        host_minio = "minio",
        porta_minio = 9000,
        minio_access_key = "minio_access_key",
        minio_secret_key = "minio_secret_key"
    )
    
    pyspark_minio.pyspark_write_parquet_overwrite(df_tabela_atual, 'gold', 'tabela_atual')
    pyspark_minio.pyspark_write_parquet_overwrite(df_tabela_historico, 'gold', 'tabela_historico_base')
    print('Ingestao da no Minio realizado com Sucesso')