from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from classes.pyspark_minio import PysparkMinio
from classes.captura_dados_alterados import CapturaDadosAlterados

if __name__ == "__main__":
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/code/jars/aws-java-sdk-bundle-1.11.1026.jar, /code/jars/hadoop-aws-3.3.2.jar") \
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    pyspark_minio = PysparkMinio(
        spark_session = spark,
        host_minio = "minio",
        porta_minio = 9000,
        minio_access_key = "minio_access_key",
        minio_secret_key = "minio_secret_key"
    )
    dataframe_historico = pyspark_minio.pyspark_read_parquet('gold', 'tabela_historico_base') 
    dataframe_recente = pyspark_minio.pyspark_read_parquet('gold', 'tabela_atual') 

    captura_dados_alterados = CapturaDadosAlterados(dataframe_recente, dataframe_historico, 'id')
    dataframe_desativado = captura_dados_alterados.gera_dataframe_apenas_itens_desativados()
    dataframe_apenas_itens_editados_desativados_json = captura_dados_alterados.gera_dataframe_apenas_itens_editados_desativados_json()
    dataframe_apenas_itens_editados = captura_dados_alterados.gera_dataframe_apenas_itens_editados()
    dataframe_itens_novos = captura_dados_alterados.gera_dataframe_itens_novos()
    ls_dataframes = [dataframe_desativado, dataframe_apenas_itens_editados_desativados_json, dataframe_apenas_itens_editados, dataframe_itens_novos]
    dataframe_final = captura_dados_alterados.unir_lista_dataframes(ls_dataframes)

    pyspark_minio.pyspark_write_parquet_overwrite(dataframe_final, 'gold', 'tabela_historico')
    print('Historico gerado com sucesso e arquivo salvo no Minio')