# Lista de Classes #

# Erro de volumetria zerada
class EmptyDataframeError(Exception):
  def __init__(self, msg):
    self.msg = msg
