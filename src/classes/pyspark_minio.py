from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

class PysparkMinio:
    """
    Uma classe para interagir com dados na camada de armazenamento Minio usando PySpark.

    Attributes:
        spark_session (SparkSession): A sessão Spark para a interação com PySpark.
        host_minio (str): O host do servidor Minio.
        porta_minio (str): A porta do servidor Minio.
        minio_access_key (str): A chave de acesso para autenticação no Minio.
        minio_secret_key (str): A chave secreta para autenticação no Minio.
    """
    def __init__(self, spark_session: SparkSession, host_minio: str, porta_minio: int, minio_access_key: str, minio_secret_key: str):
        self.spark_session = spark_session
        self.host_minio = host_minio
        self.porta_minio = porta_minio
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.conecta_minio()

    def conecta_minio(self):
        """
        Configura a conexão com o servidor Minio usando as credenciais fornecidas na inicialização da classe.
        """
        sc = self.spark_session.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.minio_access_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.minio_secret_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint",  f"{self.host_minio}:{self.porta_minio}")
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    def pyspark_read_csv(self, bucket: str, arquivo: str) -> DataFrame:
        """
        Lê um arquivo CSV do Minio e retorna um DataFrame PySpark.

        Args:
            bucket (str): O nome do bucket no Minio.
            arquivo (str): O nome do arquivo no bucket.

        Returns:
            DataFrame: O DataFrame PySpark com os dados do arquivo CSV.
        """
        url = f"s3a://{bucket}/{arquivo}"
        df = self.spark_session.read.option('header', 'true').option('inferSchema', 'true').csv(url)
        return df
    
    def pyspark_read_parquet(self, bucket: str, arquivo: str) -> DataFrame:
        """
        Lê um arquivo Parquet do Minio e retorna um DataFrame PySpark.

        Args:
            bucket (str): O nome do bucket no Minio.
            arquivo (str): O nome do arquivo no bucket.

        Returns:
            DataFrame: O DataFrame PySpark com os dados do arquivo Parquet.
        """
        url = f"s3a://{bucket}/{arquivo}"
        df = self.spark_session.read.option('header', 'true').option('inferSchema', 'true').parquet(url)
        return df
    
    def pyspark_read_json(self, bucket: str, arquivo: str) -> DataFrame:
        """
        Lê um arquivo JSON do Minio e retorna um DataFrame PySpark.

        Args:
            bucket (str): O nome do bucket no Minio.
            arquivo (str): O nome do arquivo no bucket.

        Returns:
            DataFrame: O DataFrame PySpark com os dados do arquivo JSON.
        """
        url = f"s3a://{bucket}/{arquivo}"
        df = self.spark_session.read.option('header', 'true').option('inferSchema', 'true').json(url)
        return df

    def pyspark_write_parquet_overwrite(self, df: DataFrame, bucket: str, arquivo: str) -> DataFrame:
        """
        Escreve um DataFrame PySpark no formato Parquet no Minio, substituindo qualquer arquivo existente.

        Args:
            df (DataFrame): O DataFrame PySpark a ser escrito.
            bucket (str): O nome do bucket no Minio.
            arquivo (str): O nome do arquivo no bucket.
        """
        url = f"s3a://{bucket}/{arquivo}"
        df.write.mode("overwrite").parquet(url)