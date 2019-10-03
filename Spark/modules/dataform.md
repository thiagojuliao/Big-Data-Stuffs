# Dataform Object


>__Descrição__: Objeto python criado para facililtar na manipulação e tratamento de dados em camadas que recebem diretamente os dados brutos (pousos).

>__Parâmetros de Instanciação__: Um Dataframe de origem / Uma lista de dicionários representando o Metadados (opcional).

>__Módulos__: O objeto é divido em módulos únicos com suas próprias funcionalidades. Estes por sua vez são:

Módulo | Descrição | Nome do método de classe| Exemplo de Parametrização
-------|-----------|-------------------------|---------------
__Caster__|Realiza alterações na tipagem das variáveis. Utiliza estrutura de tipagem herdada de *__pyspark.sql.types__*|__castedAs()__|__tipos__ = {"AnonID": IntegerType(), "QueryTime": TimestampType(), "ItemRank": IntegerType()}
__Coalescer__|Realiza tratamento de valores nulos através de um valor ou expressão *SQL*.|__coalescedAs()__|__se_nulos__ = {"ClickURL": "CASE WHEN ClickURL IS NULL THEN '' ELSE ClickURL END", "QueryTime": "0001-01-01 00:00:00"}
__Domain__|Insere ou captura informações sobre domínios de variáveis. Para capturar os valores apenas repita o nome da variável.|__domainAs()__|__dominios__ = {"AnonID": "Dominio infinito.", "ItemRank": "Dominio infinito."}
__Transformer__|Cria ou modifica variáveis. É possível criar variáveis com mais de um grau de derivação. Utilize sempre de expressões *SQL*.|__transformedAs()__|__trans__ = {"QueryDate": "CAST(QueryTime AS DATE)", "Query": "UPPER(Query)", "QueryDay": "DAY(QueryDate)"}
__Namer__|Renomeia variáveis.|__namedAs()__|__nomes__ = {"AnonID": "id", "Query": "query", "QueryTime": "query_time", "ItemRank": "item_rank", "ClickURL": "click_url", "QueryDate": "query_date", "QueryDay": "query_day"}

>__Propriedades do Objeto (@property)__:
>* __metadata__: Retorna o metadados em formato de texto puro.
>* __dataframe__: Retorna o dataframe com as transformações e tratamentos estipulados pelo usuário. É um objeto a parte do dataframe de origem utilizado na criação da instância do objeto.

>__Métodos Públicos__:

Método | Descrição | Parâmetros
-------|-----------|-----------
__viewMetadata()__|Mostra em formato de dataframe o metadados para o usuário.|Escolha se o dataframe deve ser mostradotruncando as colunas ou não. __Default(True)__.
__saveMetadata()__|Exporta em formato *JSON* o metadados do objeto Dataform. Este mesmo metadados serve de entrada para automaticamente construir todas as variáveis que foram criadas em uma execução anterior. Se for este o caso, utilizar da função formatadora de metadados __uploadMetadata()__ externa ao objeto Dataform.| Caminho do diretório para exportação.
