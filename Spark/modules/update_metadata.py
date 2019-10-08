def readMetadata(dir):
  
  import json
  
  # Lendo o metadados do diretório especificado
  metadados = []
  
  for linha in spark.sparkContext.textFile(dir).collect():
    info = json.loads(linha)

    # Faz o tratamento dos campos com conteúdo 'None' e monta o array da janela de tempo
    for chave in info:
      if info[chave] == "None":
        info[chave] = None
        
      if chave == "janela_de_tempo" and info[chave]:
        string = info[chave]
        range00 = int(string.split("([")[1].split(",")[0])
        range01 = int(string.split("([")[1].split(",")[1].split("]")[0])
        unidade = string.split("([")[1].split(",")[2].split(")")[0].strip().replace("'", "")
        info[chave] = [(range00, range01), unidade]
        
    metadados.append(info)
  
  return metadados
