
class Book(Dataform):
    
    depara_dias_segundos = {"d": 1, "s": 7, "m": 30, "a": 365, "h": 3600, "min": 60, "seg": 1}
    
    def __init__(self, Dataframe, visao, data, Metadata=None):
        
        self.Dataframe = Dataframe.select(visao).withColumn("dt_proc", current_date()).dropDuplicates().orderBy(visao)
        self.origem = Dataframe
        self.Metadata = []
        self.chave_primaria = visao
        self.chave_temporal = data
        
        if Metadata:
            self.Metadata = Metadata
            
            # A partir do metadados fornecido pelo usuário iniciaremos a construção do book
            pass
    
    
    
    # Constrói o público específico para a variável
    def __buildPublic__(self, dict):
        if dict["janela_de_tempo"][1] in ["d", "s", "m", "a"]:
            inicio = date_add(current_date(), dict["janela_de_tempo"][0][0] * self.depara_dias_segundos[dict["janela_de_tempo"][1]])
            fim = date_add(current_date(), dict["janela_de_tempo"][0][1] * self.depara_dias_segundos[dict["janela_de_tempo"][1]])
            
            publico = self.origem \
                    .select(self.chave_primaria, dict["origem"], self.chave_temporal) \
                    .filter(col(self.chave_temporal).between(inicio, fim))
        else:
            inicio = unix_timestamp() + dict["janela_de_tempo"][0][0] * self.depara_dias_segundos[dict["janela_de_tempo"][1]]
            fim = unix_timestamp() + dict["janela_de_tempo"][0][1] * self.depara_dias_segundos[dict["janela_de_tempo"][1]]
            
            publico = self.origem \
                    .select(self.chave_primaria, dict["origem"], self.chave_temporal) \
                    .filter(unix_timestamp(col(self.chave_temporal)).between(inicio, fim))
        
        # Se tivermos que retirar duplicidade
        if dict["duplicidade"]:
            publico = publico.dropDuplicates(dict["duplicidade"])
            
        publico.select(min(self.chave_temporal), max(self.chave_temporal)).show()
        
        return publico
    
    
    # Módulo de construção de variáveis
    def bookedAs(self, lst):
        vars_com_origem = [var for var in lst if var["origem"]]
        vars_sem_origem = [var for var in lst if not var["origem"]]
        
        # Inicia a construção das variáveis que possuem origem
        for info in vars_com_origem:
            
            # Etapa 01 - Gera Público
            publico = self.__buildPublic__(info)
            
            # Etapa 02 - Agregação
            agreg = publico.groupBy(self.chave_primaria).agg(info["agregacao"].alias(info["nome"]))
            
            # Etapa 03 - Recupera Público
            self.Dataframe = self.Dataframe.join(agreg, on = [self.chave_primaria], how = "left")
            
    
