class Dataform:
    
    def __init__(self, dataframe, colunas):
        
        self.dataframe = dataframe.select(colunas)
        
        # --- METADATA --- #
        self.metadata = [None, None, None, None, None, None, None]
        self.coluna_transformada = {} #0
        self.descricao = {} #1
        self.tipo = {} #2
        self.valor_se_nulo = {} #3
        self.dominios = {} #4
        self.transformacoes = {} #5
        self.observacoes = {} #6
        
        # Verifica na listagem de colunas de entrada se temos colunas do tipo struct, ou seja, escritas da seguinte forma colunaPai.colunaFilha.
        # Nestes casos sempre pegar o último nome da hierarquia.
        self.colunas = []
        
        for coluna in colunas:
            splitted = coluna.split(".")
            self.colunas.append(splitted[-1])
            self.coluna_transformada[splitted[-1]] = splitted[-1]
            self.descricao[splitted[-1]] = None
            self.tipo[splitted[-1]] = StringType()
            self.valor_se_nulo[splitted[-1]] = None
            self.dominios[splitted[-1]] = None
            self.transformacoes[splitted[-1]] = None
            self.observacoes[splitted[-1]] = None
        self.metadata[0] = self.coluna_transformada
        self.metadata[1] = self.descricao
        self.metadata[2] = self.tipo
        self.metadata[3] = self.valor_se_nulo
        self.metadata[4] = self.dominios
        self.metadata[5] = self.transformacoes
        self.metadata[6] = self.observacoes
    
    
    @property
    # Retorna o metadados
    def metadata(self):
        return self.metadata
        
        
    @property
    # Retorna o dataframe
    def dataframe(self):
        return self.dataframe
    
    
    # Contrói o dataframe do metadados (private method)
    def __buildMetadata__(self):
        meta_header = ["Destino <=== Origem", "Descrição", "Tipo", "Valor/Expressão se Nulo", "Domínios", "Transformações", "Observações"]
        lst_rows = []

        for key in self.metadata[0].keys():
            row = []
    
            for header in range(len(meta_header)):
                if header == 0:
                    row.append(self.metadata[0][key] + " <=== " + key)
                else:
                    row.append(str(self.metadata[header][self.metadata[0][key]]))
                lst_rows.append(row)
        
        meta_df = spark.createDataFrame(lst_rows, meta_header) \
            .withColumn("Versao", current_date()) \
            .withColumn("meta_version", current_date()) \
            .dropDuplicates()
        
        return meta_df
    
    
    # Update do metadados (private method)
    def __metadataUpdate__(self, dict, info, index):
        for coluna in dict.keys():
            if index == 0:
                try:
                    self.metadata[index][coluna]
                    self.coluna_transformada[coluna] = str(dict[coluna])
                    
                    self.descricao[dict[coluna]] = self.descricao[coluna]
                    self.tipo[dict[coluna]] = self.tipo[coluna]
                    self.valor_se_nulo[dict[coluna]] = self.valor_se_nulo[coluna]
                    self.dominios[dict[coluna]] = self.dominios[coluna]
                    self.transformacoes[dict[coluna]] = self.transformacoes[coluna]
                    self.observacoes[dict[coluna]] = self.observacoes[coluna]
                
                    del self.descricao[coluna]
                    del self.tipo[coluna]
                    del self.valor_se_nulo[coluna]
                    del self.dominios[coluna]
                    del self.transformacoes[coluna]
                    del self.observacoes[coluna]
                except:
                    try:
                        new_var = True
                        for key in self.coluna_transformada.keys():
                            if self.coluna_transformada[key] == coluna:
                                self.coluna_transformada[key] = str(dict[coluna])
                                
                                self.descricao[dict[coluna]] = self.descricao[coluna]
                                self.tipo[dict[coluna]] = self.tipo[coluna]
                                self.valor_se_nulo[dict[coluna]] = self.valor_se_nulo[coluna]
                                self.dominios[dict[coluna]] = self.dominios[coluna]
                                self.transformacoes[dict[coluna]] = self.transformacoes[coluna]
                                self.observacoes[dict[coluna]] = self.observacoes[coluna]
                                
                                del self.descricao[coluna]
                                del self.tipo[coluna]
                                del self.valor_se_nulo[coluna]
                                del self.dominios[coluna]
                                del self.transformacoes[coluna]
                                del self.observacoes[coluna]
                                
                                new_var = False
                                break
                        
                        if new_var:
                            raise ValueError
                    except:
                        self.coluna_transformada[coluna] = None
            else:
                info[coluna] = str(dict[coluna])
            
        self.metadata[index] = info
        
        
    # Tratando a tipagem dos campos escolhidos pelo usuário
    def castedAs(self, dict):
        lst_casted = []
        
        for coluna in self.colunas:
            if coluna in dict.keys():
                casted = col(coluna).cast(dict[coluna])
                aliased = casted.alias(coluna)
            else:
                aliased = col(coluna)
            lst_casted.append(aliased)
            
        self.dataframe = self.dataframe.select(lst_casted)
        self.colunas = self.dataframe.columns
        
        self.__metadataUpdate__(dict, self.tipo, 2)
        return self.dataframe
        
    
    # Tratando os valores nulos dos campos escolhidos pelo usuário
    def coalescedAs(self, dict):
        lst_coalesced = []
            
        for coluna in self.colunas:
            if coluna in dict.keys():
                try:
                    self.dataframe.select(dict[coluna])
                    coalesced = coalesce(col(coluna), dict[coluna])
                except:
                    coalesced = coalesce(col(coluna), lit(dict[coluna]))
                aliased = coalesced.alias(coluna)
            else:
                aliased = col(coluna)
            lst_coalesced.append(aliased)
            
        self.dataframe = self.dataframe.select(lst_coalesced)
        self.colunas = self.dataframe.columns
        
        self.__metadataUpdate__(dict, self.valor_se_nulo, 3)
        return self.dataframe
        
    
    # Realiza transformações dos campos escolhidos pelo usuário. Se o campo não existe na base ele é criado a partir de uma derivação.
    def transformedAs(self, dict):
        lst_transformed = []
        
        # Caso 01 - Coluna já existe no dataframe e só precisa ser transformada
        for coluna in self.colunas:
            if coluna in dict.keys():
                transformed = dict[coluna]
                aliased = transformed.alias(coluna)
            else:
                aliased = col(coluna)
            lst_transformed.append(aliased)
        
        # Caso 02 - Coluna não existe no dataframe e precisa ser criada
        for coluna in [colunas for colunas in dict.keys() if colunas not in self.colunas]:
            transformed = dict[coluna]
            aliased = transformed.alias(coluna)
            lst_transformed.append(aliased)
            self.__metadataUpdate__({coluna: None}, self.coluna_transformada, 0)
            self.__metadataUpdate__({coluna: None}, self.descricao, 1)
            self.__metadataUpdate__({coluna: StringType()}, self.tipo, 2)
            self.__metadataUpdate__({coluna: None}, self.valor_se_nulo, 3)
            self.__metadataUpdate__({coluna: None}, self.dominios, 4)
            self.__metadataUpdate__({coluna: None}, self.observacoes, 6)
            
        self.dataframe = self.dataframe.select(lst_transformed)
        self.colunas = self.dataframe.columns
        
        self.__metadataUpdate__(dict, self.transformacoes, 5)
        return self.dataframe
    
    
    # Renomeação dos campos escolhidos pelo usuário
    def renamedAs(self, dict):
        lst_renamed = []
        
        for coluna in self.colunas:
            if coluna in dict.keys():
                aliased = col(coluna).alias(dict[coluna])
            else:
                aliased = col(coluna)
            lst_renamed.append(aliased)
            
        self.dataframe = self.dataframe.select(lst_renamed)
        self.colunas = self.dataframe.columns
        
        self.__metadataUpdate__(dict, self.coluna_transformada, 0)
        return self.dataframe
    
    
    # Captura de domínios através de uma lista de variáveis introduzidas pelo usuário
    def domainAs(self, lst):
        for coluna in lst:
            values = []
            domains = self.dataframe.select(coluna).dropDuplicates().collect()
            
            for domain in domains:
                values.append(str(domain[coluna]))
                
            self.__metadataUpdate__({coluna: values}, self.dominios, 4)
            
            
    # Atualização do metadados a partir de informações inputadas pelo usuário
    def inputMetadata(self, metadata):
        infos = [self.coluna_transformada, self.descricao, self.tipo, self.valor_se_nulo, self.dominios, self.transformacoes, self.observacoes]
        
        for index, info in enumerate(metadata):
            if info and infos[index] in [self.tipo, self.valor_se_nulo, self.transformacoes]:
                if infos[index] == self.transformacoes:
                    self.transformedAs(info)
                elif infos[index] == self.tipo:
                    self.castedAs(info)
                elif infos[index] == self.valor_se_nulo:
                    self.coalescedAs(info)
                else:
                    self.transformedAs(info)
            elif info:
                self.__metadataUpdate__(info, infos[index], index)
                
        return self.metadata
    
    
    # Mostra o metadados em forma de dataframe para o usuário
    def viewMetadata(self, option=True):
        meta_df = self.__buildMetadata__()
        meta_df.drop("meta_version").show(meta_df.count(), option)
        
    
    # Grava o metadados no destino especificado pelo usuário. Formato csv particionado pela data de processamento.
    def saveMetadata(self, destino):
        meta_df = self.__buildMetadata__()
        meta_df.coalesce(1).write.partitionBy("meta_version").mode("overwrite").format("csv").option("header", True).save(destino)
        
        print("> Metadados salvo com sucesso no diretório: {}".format(destino))


# End of Class   
