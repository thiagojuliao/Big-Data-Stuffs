from datetime import *

class Dataform:
  
  # Lista com os deparas de tipagem
  depara = {"NullType()": NullType(), "StringType()": StringType(), "BinaryType()": BinaryType(), "BooleanType()": BooleanType(), "DateType()": DateType(), "TimestampType()": TimestampType(),
"DecimalType()": DecimalType(), "DoubleType()": DoubleType(), "FloatType()": FloatType(), "ByteType()": ByteType(), "IntegerType()": IntegerType(), "LongType()": LongType(), "ShortType()": ShortType(),
"ArrayType(NullType())": ArrayType(NullType()), "ArrayType(StringType())": ArrayType(StringType()), "ArrayType(BinaryType())": ArrayType(BinaryType()), "ArrayType(BooleanType())": ArrayType(BooleanType()), "ArrayType(DateType())": ArrayType(DateType()), "ArrayType(TimestampType())": ArrayType(TimestampType()), "ArrayType(DecimalType())": ArrayType(DecimalType()), "ArrayType(DoubleType())": ArrayType(DoubleType()), "ArrayType(FloatType())": ArrayType(FloatType()), "ArrayType(ByteType())": ArrayType(ByteType()), "ArrayType(IntegerType())": ArrayType(IntegerType()), "ArrayType(LongType())": ArrayType(LongType()), 
"ArrayType(ShortType())": ArrayType(ShortType())}
  
  # Referência de processamento
  ref = str(datetime.today())[0:10]
  
  def __init__(self, Dataframe, Metadata=None):
    
    if not Metadata:
      self.Dataframe = Dataframe
      self.Metadata = []
      
      # Vamos inicializar a construção do metadata de cada variável
      for coluna in self.Dataframe.columns:
        info = {
          "nome": coluna,
          "origem": coluna,
          "tipo": "StringType()",
          "valor_se_nulo": None,
          "dominio": None,
          "transformacao": None,
          "versao": self.ref
        }
        self.Metadata.append(info)
    else:
        self.Metadata = Metadata
        
        # A partir de um dado metadados fornecido pelo usuário, iniciaremos o Dataforming
        # Etapa 01 - Pré seleção das colunas de origem
        infos_com_origem = [info for info in self.Metadata if info["origem"]]
        colunas = [info["origem"] for info in infos_com_origem]
        self.Dataframe = Dataframe.select(colunas)
        
        # Etapa 02 - Inicializa os módulos (Caster, Coalescer, Transformer, Domain, Namer)
        # Faremos primeiro os casos sem origem
        infos_sem_origem = [info for info in self.Metadata if not info["origem"]]
        infos_de_grau_maiorigual_um = []
        
        for info in infos_sem_origem:
          try:
            self.transformedAs({info["nome"]: info["transformacao"]}, True)
            self.coalescedAs({info["nome"]: info["valor_se_nulo"]}, True)
            self.castedAs({info["nome"]: self.depara[info["tipo"]]}, True)
            self.domainAs({info["nome"]: info["dominio"]}, True)
          except:
            infos_de_grau_maiorigual_um.append(info)
            
        # Agora os casos que possuem origem
        for info in infos_com_origem:
          self.transformedAs({info["origem"]: info["transformacao"]}, True)
          self.coalescedAs({info["origem"]: info["valor_se_nulo"]}, True)
          self.castedAs({info["origem"]: self.depara[info["tipo"]]}, True)
          self.domainAs({info["origem"]: info["dominio"]}, True)
          self.namedAs({info["origem"]: info["nome"]}, True)
          
        # Por fim executa os módulos para as variáveis de grau maior ou iguais a 1
        index = 0
        
        while infos_de_grau_maiorigual_um:
          info = infos_de_grau_maiorigual_um[index]
          
          try:
            self.transformedAs({info["nome"]: info["transformacao"]}, True)
            self.coalescedAs({info["nome"]: info["valor_se_nulo"]}, True)
            self.castedAs({info["nome"]: self.depara[info["tipo"]]}, True)
            self.domainAs({info["nome"]: info["dominio"]}, True)
            infos_de_grau_maiorigual_um.remove(info)
            index = 0
          except:
            index += 1

            
  # Método de atualização do metadados
  def __updateMetadata__(self, col_list, dict_list):
    # Verifica e insere informações novas
    for coluna in [nova_coluna for nova_coluna in col_list if nova_coluna not in self.Dataframe.columns]:
      self.Metadata.append({"nome": coluna, "origem": None, "tipo": "StringType()", "valor_se_nulo": None, "dominio": None, "transformacao": None, "versao": self.ref})
    
    # Atualiza de fato o metadados
    for info in self.Metadata:
      if info["nome"] in col_list:
        index = col_list.index(info["nome"])
        dict = dict_list[index]
        
        for chave in dict.keys():
          info[chave] = dict[chave]
      
      
  # Método de construção do dataframe para o metadados
  def __buildMetadataDF__(self):
    # Construção do header
    meta_header = []
    for chave in self.Metadata[0].keys():
      meta_header.append(chave)
    
    # Construção das linhas
    lst_rows = []
    for info in self.Metadata:
      row = []
      
      for valor in info.values():
        row.append(str(valor))
      lst_rows.append(row)
      
    # Constrói o dataframe
    meta_DF = spark.createDataFrame(lst_rows, meta_header)

    return meta_DF
    
    
  @property # Retorna o metadados
  def metadata(self):
    return self.Metadata
  
  
  @property # Retorna o dataframe
  def dataframe(self):
    return self.Dataframe
  
  
  # Módulo 01 - Alteração de tipagem dos campos
  def castedAs(self, dict, imported_metadata=False):
    lst_casted = []
    
    for coluna in self.Dataframe.columns:
      if coluna in dict.keys() and dict[coluna]:
        casted = col(coluna).cast(dict[coluna])
        aliased = casted.alias(coluna)
        if not imported_metadata:
          self.__updateMetadata__([coluna], [{"tipo": str(dict[coluna]) + "()", "versao": self.ref}])
      else:
        aliased = col(coluna)
      lst_casted.append(aliased)
      
    self.Dataframe = self.Dataframe.select(lst_casted)
    return self.Dataframe
  
  
  # Módulo 02 - Tratamento de valores nulos
  def coalescedAs(self, dict, imported_metadata=False):
    lst_coalesced = []
    
    for coluna in self.Dataframe.columns:
      if coluna in dict.keys():
        # Devemos checar primeiro se temos um valor a inserir ou uma expressão envolvendo uma ou mais colunas
        try:
          self.Dataframe.select(expr(dict[coluna]))
          coalesced = coalesce(col(coluna), expr(dict[coluna]))
        except:
          coalesced = coalesce(col(coluna), lit(dict[coluna]))
        finally:
          aliased = coalesced.alias(coluna)
          if not imported_metadata:
            self.__updateMetadata__([coluna], [{"valor_se_nulo": dict[coluna], "versao": self.ref}])
      else:
          aliased = col(coluna)
      lst_coalesced.append(aliased)
        
    self.Dataframe = self.Dataframe.select(lst_coalesced)
    return self.Dataframe
  
  
  # Módulo 03 - Captura ou inserção da listagem de domínios
  def domainAs(self, dict, imported_metadata=False):
    if not imported_metadata:
      for info in self.Metadata:
        if info["nome"] in dict.keys():
          # Verifica se necessitamos gerar este domínio através de um agrupamento na variável
          try:
            dominio = []
            valores = self.Dataframe.select(dict[info["nome"]]).distinct().collect()

            for valor in valores:
              dominio.append(valor[info["nome"]])
            info["dominio"] = dominio
          except:
            info["dominio"] = dict[info["nome"]]
          info["versao"] = self.ref
          
    return self.Metadata
  
  
  # Módulo 04 - Aplica transformações em colunas existentes ou cria colunas novas
  def transformedAs(self, dict, imported_metadata=False):
    lst_transformed = []
    
    # Caso 01 - Coluna já existente no dataframe
    for coluna in self.Dataframe.columns:
      if coluna in dict.keys() and dict[coluna]:
        transformed = expr(dict[coluna])
        aliased = transformed.alias(coluna)
        if not imported_metadata:
          self.__updateMetadata__([coluna], [{"transformacao": dict[coluna], "versao": self.ref}])
      else:
        aliased = col(coluna)
      lst_transformed.append(aliased)
        
    # Caso 02 - Uma nova coluna a ser criada
    for coluna in [nova_coluna for nova_coluna in dict.keys() if nova_coluna not in self.Dataframe.columns]:
      transformed = expr(dict[coluna]).cast(StringType())
      aliased = transformed.alias(coluna)
      lst_transformed.append(aliased)
      if not imported_metadata:
        self.__updateMetadata__([coluna], [{"transformacao": dict[coluna], "versao": self.ref}])
        
    self.Dataframe = self.Dataframe.select(lst_transformed)
    return self.Dataframe
  
  
  # Módulo 05 - Renomeação de variáveis
  def namedAs(self, dict, imported_metadata=False):
    lst_named = []
    
    for coluna in self.Dataframe.columns:
      if coluna in dict.keys() and dict[coluna]:
        aliased = col(coluna).alias(dict[coluna])
        if not imported_metadata:
          self.__updateMetadata__([coluna], [{"nome": dict[coluna], "versao": self.ref}])
      else:
        aliased = col(coluna)
      lst_named.append(aliased)
    
    self.Dataframe = self.Dataframe.select(lst_named)
    return self.Dataframe
  
  
  # Visualização do Metadados
  def viewMetadata(self, truncate=True):
    meta_DF = self.__buildMetadataDF__()
    meta_DF = meta_DF.select(col("nome").alias("Nome"), col("origem").alias("Origem"), col("tipo").alias("Tipo"), col("valor_se_nulo").alias("Valor/Expressão(SQL) se Nulo"), 
                             col("dominio").alias("Domínio"), col("transformacao").alias("Transformação(SQL)"), col("versao").alias("Versão"))
    
    return meta_DF.show(len(self.Metadata), truncate)
  
  
  # Exportação do metadados para um diretório especificado pelo usuário
  def saveMetadata(self, dir):
    meta_DF = self.__buildMetadataDF__()
    meta_DF.coalesce(1).write.mode("overwrite").format("json").save(dir)
    
    return "> Metadados exportado com SUCESSO para o diretório: {}.".format(dir)
  
 
# End of class
