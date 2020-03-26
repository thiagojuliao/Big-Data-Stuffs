from pyspark.sql.utils import AnalysisException


class StructParser:
  
  def __init__(self, dataframe):
    self.df = dataframe
    self.schema = dataframe.schema
    self.cols = []
  
  
  def __get_children__(self, parent, parent_name):
    if isinstance(parent.dataType, StructType):
      return parent.dataType.fields
    elif isinstance(parent.dataType, ArrayType):
      if not isinstance(parent.dataType.elementType, StructType):
        self.cols.append(parent_name)
      else:
        return parent.dataType.elementType.fields
    else:
        self.cols.append(parent_name)

        
  def __build_tree__(self, parent, parent_name):
    if not (isinstance(parent.dataType, StructType) or isinstance(parent.dataType, ArrayType)):
        try:
          self.df.select(parent_name)
          self.cols.append(parent_name)
        except:
          splitted = parent_name.split(".")
          parent_name = ".".join(splitted[0:-1])
          
          if parent_name not in self.cols:
            self.cols.append(parent_name)
    else:
      children = self.__get_children__(parent, parent_name)
      
      if children:
        for child in children:
          self.__build_tree__(child, parent_name + "." + child.name)

  
  def __get_alias__(self, col):
    return col.lower().replace(".", "_")
  
  
  def __normalize__(self):
    self.cols = [col(column).alias(self.__get_alias__(column)) for column in self.cols]
  
  
  def __clean__(self):
    cleaned = []
    
    for column in self.cols:
      try:
        self.df.select(column)
        cleaned.append(column)
      except AnalysisException:
        s = str(column).split("'")[1].split("AS")[0].rstrip()
        print("*** WARNING *** {} cannot be extracted possibily caused by a bad schema.".format(s))
        
    return cleaned
   
    
  def parse(self):
    """
      Parses the target dataframe returning a new dataframe with all columns extracted from struct typed objects.
      Also works with struct objects inside arrays on most cases.
    """
    for field in self.schema:
      self.__build_tree__(field, field.name)
    
    self.__normalize__()
    self.cols = self.__clean__()
    
    finalDF = self.df.select(self.cols)
    struct_cols = [field.name for field in finalDF.schema if isinstance(field, StructType)]
    
    return finalDF.drop(*struct_cols)
