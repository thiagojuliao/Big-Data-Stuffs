
class Book(Dataform):
    
    depara_dias_segundos = {"d": 1, "s": 7, "m": 30, "a": 365, "h": 3600, "min": 60, "seg": 1}
    
    def __init__(self, origem, visao, referencia, Metadata=None, Publico=None):
        
        # Define o público base do book
        if not Publico:
            self.Dataframe = origem.select(visao).withColumn("dt_proc", current_date()).withColumn(referencia, current_date()).dropDuplicates().orderBy(visao)
            self.Publico = origem.select(visao).withColumn(referencia, current_date()).dropDuplicates().orderBy(visao)
        else:
            self.Dataframe = Publico.withColumn("dt_proc", current_date()).orderBy(visao, referencia)
            self.Publico = Publico
            
        self.Origem = origem
        self.Metadata = []
        self.chave_primaria = visao
        self.chave_temporal = referencia
        
        if Metadata:
            self.Metadata = Metadata
            
            # A partir do metadados fornecido pelo usuário iniciaremos a construção do book
            pass
    
    
    # Realiza extração dos dados
    def __extractor__(self, ref, dict):
        
        # Aplica filtro(s) na origem caso necessário
        if dict["filtro"]:
          filtrada = self.Origem.select(self.chave_primaria, dict["origem"], dict["variavel_temporal"]).filter(dict["filtro"])
        else:
          filtrada = self.Origem.select(self.chave_primaria, dict["origem"], dict["variavel_temporal"])
        
        # Verifica se após o filtro a base filtrada possui volumetria, caso contrário aborta o processo e informa ao usuário
        if filtrada.limit(1).count() == 0:
          raise EmptyDataframeError("O filtro para esta variável resultou em uma volumetria zerada. Verificar se a construção do filtro está correta.")
          
        # Aplica janelas de tempo
        if dict["janela_de_tempo"][1] in ["d", "s", "m", "a"]:
            inicio = date_add(lit(ref), dict["janela_de_tempo"][0][0] * self.depara_dias_segundos[dict["janela_de_tempo"][1]])
            fim = date_add(lit(ref), dict["janela_de_tempo"][0][1] * self.depara_dias_segundos[dict["janela_de_tempo"][1]])
            
            extracao = filtrada.filter(col(dict["variavel_temporal"]).between(inicio, fim))
        else:
            inicio = unix_timestamp(lit(ref)) + dict["janela_de_tempo"][0][0] * self.depara_dias_segundos[dict["janela_de_tempo"][1]]
            fim = unix_timestamp(lit(ref)) + dict["janela_de_tempo"][0][1] * self.depara_dias_segundos[dict["janela_de_tempo"][1]]
            
            extracao = filtrada.filter(unix_timestamp(col(dict["variavel_temporal"])).between(inicio, fim))
        
        # Verifica e remove duplicidade
        if dict["duplicidade"]:
            extracao = extracao.dropDuplicates(dict["duplicidade"])
        
        return extracao.withColumn(self.chave_temporal, lit(ref))
    
    
    # Módulo de construção de variáveis
    def bookedAs(self, lst):
        vars_com_origem = [var for var in lst if var["origem"] and var["nome"] not in self.Dataframe.columns]
        vars_sem_origem = [var for var in lst if not var["origem"] and var["nome"] not in self.Dataframe.columns]
        
        # Etapa 01 - Gera vetor de datas para extração
        vetor_de_datas = [str(data[self.chave_temporal]) for data in self.Publico.select(self.chave_temporal).orderBy(self.chave_temporal).dropDuplicates().collect()]
            
        # Inicia a construção das variáveis que possuem origem
        for info in vars_com_origem:
            
            aux = vetor_de_datas
            
            # Etapa 02 - Extração
            extracao = self.__extractor__(aux[0], info)
            aux.remove(aux[0])
            
            for ref in vetor_de_datas:
                extracao = extracao.union(self.__extractor__(ref, info))
                
            # Etapa 03 - Agregação
            agregacao = extracao.groupBy(self.chave_primaria, self.chave_temporal).agg(expr(info["agregacao"]).alias(info["nome"]))
            
            # Etapa 04 - Recupera Público
            self.Dataframe = self.Dataframe.join(agregacao, on = [self.chave_primaria, self.chave_temporal], how = "left")
            
