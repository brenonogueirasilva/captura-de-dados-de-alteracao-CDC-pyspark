from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, array, udf, lit, when, current_date,  when
from pyspark.sql.types import ArrayType, IntegerType, StringType, FloatType, DoubleType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Row


class CapturaDadosAlterados:
    """
    Uma classe para capturar e manipular dados alterados entre duas tabelas, simulando CDC em um banco relacional.

    Attributes:
        dataframe_recente (DataFrame): O DataFrame contendo os dados mais recentes.
        dataframe_historico (DataFrame): O DataFrame contendo o histórico de dados.
        coluna_id (str): O nome da coluna que serve como identificador único.
    """
    def __init__(self, dataframe_recente: DataFrame, dataframe_historico: DataFrame, coluna_id: str):
        self.dataframe_recente = dataframe_recente
        self.dataframe_historico = dataframe_historico
        self.coluna_id = coluna_id
        self.colunas = list(self.dataframe_recente.columns.copy())
        self.colunas.remove(self.coluna_id)
        if 'ativo' not in self.colunas:
            self.dataframe_historico = self.criar_colunas_meta_data(self.dataframe_historico)

    def filtra_registros_ativos(self, dataframe: DataFrame) -> DataFrame:
        """
        Filtra registros ativos no DataFrame, através da coluna Ativo

        Args:
            dataframe (DataFrame): O DataFrame a ser filtrado.

        Returns:
            DataFrame: O DataFrame resultante com apenas registros ativos.
        """
        try:
            return dataframe.filter(col('ativo'))
        except:
            return dataframe
        
    def filtra_registros_desativados(self, dataframe: DataFrame) -> DataFrame:
        """
        Filtra registros desativados no DataFrame, através da coluna ativo

        Args:
            dataframe (DataFrame): O DataFrame a ser filtrado.

        Returns:
            DataFrame: O DataFrame resultante com apenas registros desativados.
        """
        try:
            dataframe = dataframe.filter(~col('ativo'))
            return dataframe
        except:
            return dataframe

    def filtra_pelo_id(self, dataframe: DataFrame, dataframe_ids: DataFrame) -> DataFrame:
        """
        Filtra o DataFrame com base em IDs.

        Args:
            dataframe (DataFrame): O DataFrame a ser filtrado.
            dataframe_ids (DataFrame): O DataFrame contendo IDs a serem usados como filtro.

        Returns:
            DataFrame: O DataFrame resultante após a filtragem por ID.
        """
        dataframe_ids = dataframe_ids.withColumnRenamed(self.coluna_id, 'id_auxiliar' )
        dataframe = dataframe.join( dataframe_ids, dataframe[self.coluna_id] == dataframe_ids['id_auxiliar'], 'inner').drop( dataframe_ids['id_auxiliar'])
        return dataframe

    def gera_id_novo(self) -> DataFrame:
        """
        Gera IDs para itens novos no DataFrame recente.

        Returns:
            DataFrame: O DataFrame contendo IDs para itens novos.
        """
        dataframe_id_novo = self.filtra_registros_ativos(self.dataframe_historico)
        dataframe_id_novo = self.dataframe_recente.join(dataframe_id_novo, self.dataframe_recente[self.coluna_id] == dataframe_id_novo[self.coluna_id], 'left_anti')
        dataframe_id_novo = dataframe_id_novo.select(self.coluna_id)
        return dataframe_id_novo
    
    def gera_dataframe_itens_novos(self) -> DataFrame:
        """
        Gera um DataFrame contendo apenas itens novos.

        Returns:
            DataFrame: O DataFrame resultante com itens novos.
        """
        dataframe_itens_novos = self.filtra_pelo_id(self.dataframe_recente, self.gera_id_novo())
        dataframe_itens_novos = self.criar_colunas_meta_data(dataframe_itens_novos)
        return dataframe_itens_novos

    def criar_coluna_chave(self, dataframe: DataFrame, nome_chave: str) -> DataFrame:
        """
        Cria uma coluna de chave para o DataFrame, para comparar as mudanças da colunas

        Args:
            dataframe (DataFrame): O DataFrame a ser modificado.
            nome_chave (str): O nome da coluna de chave.

        Returns:
            DataFrame: O DataFrame resultante com a coluna de chave.
        """
        ls_colunas = dataframe.columns
        ls_colunas.remove(self.coluna_id)
        if 'ativo' in ls_colunas:
            ls_colunas.remove('ativo')
            ls_colunas.remove('criado_em')
            ls_colunas.remove('editado_em')
        udf_create_list = udf(lambda x: x.split('|') , StringType())
        dataframe = dataframe.withColumn(nome_chave, concat_ws(';', *[col(col_name) for col_name in ls_colunas]))
        dataframe = dataframe.withColumn(nome_chave, concat_ws(';', udf_create_list(col(nome_chave))))
        return dataframe

    def gera_id_editado(self) -> DataFrame:
        """
        Gera IDs dos itens editados no DataFrame recente.

        Returns:
            DataFrame: O DataFrame contendo IDs para itens editados.
        """
        dataframe_historico = self.filtra_registros_ativos(self.dataframe_historico)
        dataframe_historico = self.criar_coluna_chave(dataframe_historico, 'chave2')
        dataframe_recente = self.criar_coluna_chave(self.dataframe_recente, 'chave1')
        dataframe_update = dataframe_historico.join(dataframe_recente.select(self.coluna_id, 'chave1'), dataframe_recente.select(self.coluna_id, 'chave1')[self.coluna_id] == dataframe_historico[self.coluna_id]).drop(dataframe_recente[self.coluna_id])
        dataframe_update = dataframe_update.withColumn('compara', col("chave1")  != col("chave2") )
        dataframe_update = dataframe_update.filter(col('compara'))
        dataframe_update = dataframe_update.drop('compara', 'chave1' , 'chave2' )
        dataframe_ids = dataframe_update.select(self.coluna_id)
        return dataframe_ids
    
    def gera_dataframe_apenas_itens_editados(self) -> DataFrame:
        """
        Gera um DataFrame contendo apenas itens editados.

        Returns:
            DataFrame: O DataFrame resultante com itens editados.
        """
        dataframe_editado = self.filtra_pelo_id(self.dataframe_recente, self.gera_id_editado())
        dataframe_editado = self.criar_colunas_meta_data(dataframe_editado)
        return dataframe_editado

    def gera_dataframe_apenas_itens_editados_desativados(self) -> DataFrame:
        """
        Gera um DataFrame contendo apenas itens editados que serão desativados e irao para a historico.

        Returns:
            DataFrame: O DataFrame resultante com itens editados e desativados.
        """
        dataframe_editado_desativado = self.filtra_pelo_id(self.dataframe_historico, self.gera_id_editado())
        dataframe_editado_desativado = self.criar_colunas_meta_data_desativado(dataframe_editado_desativado)
        return dataframe_editado_desativado

    def criar_colunas_meta_data(self, dataframe: DataFrame) -> DataFrame:
        """
        Cria colunas de metadados para um DataFrame, coluna ativo (saber se a linha esta ativa ou se ja foi deletada logicamente, 
        criado_em: data que o registro foi criado e editado_em ultima data que o registro foi editado) 
        o metodo torna a coluna ativo e seta a criacao para a data atual)

        Args:
            dataframe (DataFrame): O DataFrame a ser modificado.

        Returns:
            DataFrame: O DataFrame resultante com colunas de metadados.
        """
        dataframe = dataframe.withColumn('ativo', lit(True))
        dataframe = dataframe.withColumn('criado_em', lit(current_date()))
        dataframe = dataframe.withColumn('editado_em', lit(None))
        return dataframe
    
    def criar_colunas_meta_data_desativado(self, dataframe: DataFrame) -> DataFrame:
        """
        Cria colunas de metadados para um DataFrame, coluna ativo (saber se a linha esta ativa ou se ja foi deletada logicamente, 
        criado_em: data que o registro foi criado e editado_em ultima data que o registro foi editado)  
        o metodo torna a coluna ativo em desativado e seta o editado pela data atual a criacao para a data atual)

        Args:
            dataframe (DataFrame): O DataFrame a ser modificado.

        Returns:
            DataFrame: O DataFrame resultante com colunas de metadados.
        """
        dataframe = dataframe.withColumn('ativo', lit(False))
        dataframe = dataframe.withColumn('editado_em', lit(current_date()))
        return dataframe

    def gera_coluna_alteracoes(self) -> DataFrame:
        """
        Gera um DataFrame contendo apenas itens editados e desativados em em formato json, a a partir dessa coluna json é possivel saber as colunas
        que foram editadas e as colunas que mudaram com os valores antigos e novos. Para gerar essa coluna será necessario mais computacao que apenas
        setar as colunas de metadado

        Returns:
            DataFrame: O DataFrame resultante com itens editados e desativados em formato JSON.
        """
        def gera_json(lista_item_novo, lista_item_historico, colunas):
            dicionario = {}
            #colunas = ['categoria', 'data', 'valor']
            colunas = colunas.strip("[]").split(',')
            lista_item_novo = lista_item_novo.strip("[]").split(';')
            lista_item_historico = lista_item_historico.strip("[]").split(';')
            for indice in range(len(lista_item_novo)):
                elemento_novo = lista_item_novo[indice]
                elemento_historico = lista_item_historico[indice]
                if elemento_novo != elemento_historico:
                    chave = colunas[indice]
                    valor = [elemento_novo, elemento_historico]
                    dicionario[chave] = valor
            return dicionario
        
        dataframe_historico = self.filtra_registros_ativos(self.dataframe_historico)
        id_editado = self.gera_id_editado()
        df_json1 = self.criar_coluna_chave(self.filtra_pelo_id(self.dataframe_recente, id_editado), 'chave1')
        df_json1 = df_json1[['id', 'chave1']]
        df_json2 = self.criar_coluna_chave(self.filtra_pelo_id(dataframe_historico, id_editado), 'chave2')
        df_json2 = df_json2[['id', 'chave2']]
        df_uniao = df_json1.join(df_json2, df_json1[self.coluna_id] == df_json2["id"], 'inner').drop(df_json1[self.coluna_id])
        df_uniao = df_uniao.withColumn('colunas', lit(str(self.colunas)))
        udf_create_json = udf( gera_json, StringType())
        df_uniao = df_uniao.withColumn( 'json_alteracoes' , udf_create_json( 'chave1' ,'chave2', 'colunas'))
        df_uniao = df_uniao.select('id', 'json_alteracoes')
        return df_uniao
    
    def gera_dataframe_apenas_itens_editados_desativados_json(self) -> DataFrame:
        """
        Gera um DataFrame contendo apenas itens desativados, na comparacao entre as duas tabelas, junto a com coluna json

        Returns:
            DataFrame: O DataFrame resultante com itens desativados.
        """
        dataframe_editado = self.gera_dataframe_apenas_itens_editados_desativados()
        dataframe_coluna_alteracoes = self.gera_coluna_alteracoes()
        dataframe_coluna_alteracoes = dataframe_coluna_alteracoes.withColumnRenamed(self.coluna_id, 'id_auxiliar' )
        dataframe_editado = dataframe_editado.join( dataframe_coluna_alteracoes, dataframe_editado[self.coluna_id] == dataframe_coluna_alteracoes['id_auxiliar'], 'inner').drop( dataframe_coluna_alteracoes['id_auxiliar'])
        return dataframe_editado
    
    def gera_dataframe_apenas_itens_desativados(self) -> DataFrame:
        """
        Gera um DataFrame contendo apenas itens desativados, na comparacao entre as duas tabelas, sem a coluna json

        Returns:
            DataFrame: O DataFrame resultante com itens desativados.
        """
        dataframe= self.filtra_registros_desativados(self.dataframe_historico)
        return dataframe
    
    def unir_lista_dataframes(self, ls_dataframes: list) -> DataFrame:
        """
        Une uma lista de DataFrames.

        Args:
            ls_dataframes (list): Uma lista de DataFrames a serem unidos.

        Returns:
            DataFrame: O DataFrame resultante após a união.
        """
        for indice in range(len(ls_dataframes)):
            if indice == 0:
                dataframe = ls_dataframes[indice].alias("copia")
            else:
                dataframe = dataframe.unionByName(ls_dataframes[indice], allowMissingColumns=True)
        dataframe = dataframe.orderBy(self.coluna_id)
        return dataframe