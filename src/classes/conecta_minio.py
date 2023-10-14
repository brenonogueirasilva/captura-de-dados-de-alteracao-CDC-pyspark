from minio import Minio
from minio.error import S3Error

class ConectaMinio:
    """
    Uma classe para interagir com o servidor Minio para operações de armazenamento de objetos.

    Attributes:
        host (str): O host do servidor Minio.
        porta (int): A porta do servidor Minio.
        access_key (str): A chave de acesso para autenticação no Minio.
        secret_key (str): A chave secreta para autenticação no Minio.
    """
    def __init__(self, host: str, porta: int, access_key: str, secret_key: str):
        self.host = host 
        self.porta = porta 
        self.access_key = access_key
        self.secret_key = secret_key

    def criar_cliente_minio(self)-> Minio:
        """
        Cria e retorna um cliente Minio configurado com as credenciais fornecidas.

        Returns:
            Minio: Um cliente Minio configurado.
        """
        minio_cliente = Minio(
        f"{self.host}:{self.porta}",
        access_key = self.access_key,
        secret_key = self.secret_key,
        secure=False
        )
        return minio_cliente
    
    def inserir_arquivo(self, nome_bucket: str, nome_arquivo: str, caminho_arquivo_local: str):
        """
        Faz o upload de um arquivo local para o servidor Minio.

        Args:
            nome_bucket (str): O nome do bucket no Minio.
            nome_arquivo (str): O nome do arquivo no bucket.
            caminho_arquivo_local (str): O caminho local do arquivo a ser enviado.
        """
        minio_cliente = self.criar_cliente_minio()
        try:
            minio_cliente.fput_object(
                nome_bucket,
                nome_arquivo,  
                caminho_arquivo_local  
            )
            print("Upload realizado com sucesso!")
        except S3Error as err:
            print(err)

    def listar_bucket(self, nome_bucket: str):
        """
        Lista objetos dentro de um bucket no servidor Minio e imprime seus nomes.

        Args:
            nome_bucket (str): O nome do bucket no Minio.
        """
        minio_cliente = self.criar_cliente_minio()
        try:
            objects = minio_cliente.list_objects( nome_bucket, recursive=True)
            for obj in objects:
                print(obj.object_name)
        except S3Error as err:
            print(err)

    def criar_bucket(self, nome_bucket: str):
        """
        Cria um novo bucket no servidor Minio.

        Args:
            nome_bucket (str): O nome do novo bucket a ser criado.
        """
        try:
            minio_cliente = self.criar_cliente_minio()
            minio_cliente.make_bucket(nome_bucket)
            print('bucket criado com sucesso')
        except S3Error as err:
            print(err)