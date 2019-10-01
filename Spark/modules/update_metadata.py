def uploadMetadata(dir):
  import json
  
  # Lendo o metadados do diretório especificado
  metadados = []
  
  for linha in spark.sparkContext.textFile(dir).collect():
    info = json.loads(linha)

    # Faz o tratamento dos campos com conteúdo 'None'
    for chave in info:
      if info[chave] == "None":
        info[chave] = None
    metadados.append(info)
  
  return metadados
