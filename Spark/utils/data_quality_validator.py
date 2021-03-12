class DataQualityValidator:
  # Dict contendo a tupla expressão e operador de junção
  metrics_base_expressions = {
    "is_never_null": ("{} is not null", " and "),
    "is_always_null": ("{} is null", " and "),
    "satisfies": ("({})", " and "),
    "is_matching_regex": ("{c} is null or regexp_replace({c}, '{r}', '') == ''", " and "),
    "is_any_of": ("{} in ({})", " and ")
  }
  
  # Dict contendo o texto base para o report das métricas
  metrics_base_report_text = {
    "is_never_null": "[Nullability] {} record(s) were found with null values that shoudn't be null.",
    "is_always_null": "[Reverse Nullability] {} records(s) were found with non null values that should be null.",
    "satisfies": "[Validity, Integrity] {} record(s) were found with integrity issues.",
    "is_matching_regex": "[Validity, Integrity] {} record(s) were found with inconsistent values that were not expected.",
    "is_any_of": "[Validity] {} record(s) were found with unexpected values inside a specific column domain.",
    "is_formatted_as_date": "[Validity, Integrity] {} record(s) were found that does not match the specified date format standards.",
    "has_unique_key": "[Duplicity] {} record(s) were found with duplicity on the specified keys.",
    "has_num_rows_greater_than": "[Data Volume] The number of output rows is lesser than the expected value of {} rows."
  }
  
  
  def __init__(self, dataframe):
    self.df = dataframe
    self.metrics_plan = {}
    self.flags = []
  
  
  @property
  def plan(self):
    return self.metrics_plan
  
  
  @property
  def dataframe(self):
    return self.df
  
  
  def __build_metric_expression__(self, metric):
    if self.metrics_base_expressions.get(metric, None):
      base_expression, join_operator = self.metrics_base_expressions[metric]
 
    if metric == "satisfies":
      conditionals = [base_expression.format(e) for e in self.metrics_plan["satisfies"]["expressions"]]
      metric_expression = "case when " + join_operator.join(conditionals) + " then 0 else 1 end"
    elif metric == "is_matching_regex":
      conditionals = []
      
      for regexp in self.metrics_plan["is_matching_regex"]:
        for col in self.metrics_plan["is_matching_regex"][regexp]:
          conditionals.append(base_expression.format(c=col, r=regexp))
      
      metric_expression = "case when " + join_operator.join(conditionals) + " then 0 else 1 end"
    elif metric == "is_any_of":
      conditionals = []
      
      for domain in self.metrics_plan["is_any_of"]:
        for col in self.metrics_plan["is_any_of"][domain]:
          conditionals.append(base_expression.format(col, domain.replace("[", "").replace("]", "")))
          
      metric_expression = "case when " + join_operator.join(conditionals) + " then 0 else 1 end"
    elif metric == "is_formatted_as_date":
      dummy_i_cols = []
      
      for i, date_format in enumerate(self.metrics_plan["is_formatted_as_date"]):
        dummy_j_cols = []
        
        for j, col in enumerate(self.metrics_plan["is_formatted_as_date"][date_format]):
          c_j = "dummy_j_df_{}".format(str(j).rjust(2, "0"))
      
          self.df = self.df.withColumn(c_j, check_date_format(str(col), lit(date_format)))
          dummy_j_cols.append(c_j)
        
        c_i = "dummy_i_df_{}".format(str(i).rjust(2, "0"))
        sum_j_c = " + ".join(dummy_j_cols)
        
        self.df = self.df.withColumn(c_i, expr(sum_j_c)).drop(*dummy_j_cols)
        dummy_i_cols.append(c_i)
        
      sum_i_c = " + ".join(dummy_i_cols)
      self.df = self.df.withColumn("flag_is_formatted_as_date", expr(sum_i_c)).drop(*dummy_i_cols)
      
      metric_expression = None
    elif metric == "has_unique_key":
      partitions = self.metrics_plan["has_unique_key"]
      window = Window.partitionBy(*partitions).orderBy(lit(1))
      unique_key_case_when = \
      """
        case 
          when pk_hash = lag_pk_hash then 1 else 0
        end
      """
      
      self.df = self.df \
        .withColumn("pk_hash", hash(*partitions)) \
        .withColumn("lag_pk_hash", lag("pk_hash").over(window)) \
        .withColumn("flag_has_unique_key", expr(unique_key_case_when)) \
        .drop("pk_hash", "lag_pk_hash")
      
      metric_expression = None
    elif metric == "has_num_rows_greater_than":
      metric_expression = None
    else:
      conditionals = [base_expression.format(col) for col in self.metrics_plan[metric]]
      metric_expression = "case when " + join_operator.join(conditionals) + " then 0 else 1 end"
    return metric_expression
    
  
  def isNeverNull(self, *cols):
    if self.metrics_plan.get("is_never_null", None):
      self.metrics_plan["is_never_null"].append(*cols)
    else:
      self.metrics_plan["is_never_null"] = [*cols]
    return self
  
  
  def isAlwaysNull(self, *cols):
    if self.metrics_plan.get("is_always_null", None):
      self.metrics_plan["is_always_null"].append(*cols)
    else:
      self.metrics_plan["is_always_null"] = [*cols]
    return self
  
  
  def satisfies(self, *exprs):
    if self.metrics_plan.get("satisfies", None):
      self.metrics_plan["satisfies"]["expressions"].append(*exprs)
    else:
      self.metrics_plan["satisfies"] = {}
      self.metrics_plan["satisfies"]["expressions"] = [*exprs]
    return self
  
  
  def isMatchingRegex(self, cols, regexp):
    if not isinstance(cols, list):
      cols = [cols]
      
    if self.metrics_plan.get("is_matching_regex", None):
      if self.metrics_plan["is_matching_regex"].get(regexp, None):
        self.metrics_plan["is_matching_regex"][regexp].append(*cols)
      else:
        self.metrics_plan["is_matching_regex"][regexp] = cols
    else:
      self.metrics_plan["is_matching_regex"] = {}
      self.metrics_plan["is_matching_regex"][regexp] = cols
    return self
  
  
  def isAnyOf(self, cols, domain):
    if not isinstance(domain, list):
      domain = [domain]
    
    if not isinstance(cols, list):
      cols = [cols]
      
    if self.metrics_plan.get("is_any_of", None):
      if self.metrics_plan["is_any_of"].get(str(domain), None):
        self.metrics_plan["is_any_of"][str(domain)].append(*cols)
      else:
        self.metrics_plan["is_any_of"][str(domain)] = cols
    else:
      self.metrics_plan["is_any_of"] = {}
      self.metrics_plan["is_any_of"][str(domain)] = cols
    return self
  
  
  def isFormattedAsDate(self, cols, date_format):
    if not isinstance(cols, list):
      cols = [cols]
    
    if self.metrics_plan.get("is_formatted_as_date", None):
      if self.metrics_plan["is_formatted_as_date"].get(date_format, None):
        self.metrics_plan["is_formatted_as_date"][date_format].append(*cols)
      else:
        self.metrics_plan["is_formatted_as_date"][date_format] = cols
    else:
      self.metrics_plan["is_formatted_as_date"] = {}
      self.metrics_plan["is_formatted_as_date"][date_format] = cols
    return self
    
  
  def hasUniqueKey(self, *cols):
    if self.metrics_plan.get("has_unique_key", None):
      self.metrics_plan["has_unique_key"].append(*cols)
    else:
      self.metrics_plan["has_unique_key"] = [*cols]
    return self
  
  
  def hasNumRowsGreaterThan(self, value=0):
    if self.metrics_plan.get("has_num_rows_greater_than", None):
      self.metrics_plan["has_num_rows_greater_than"] = value
    else:
      self.metrics_plan["has_num_rows_greater_than"] = value
    return self
  
  
  def __build_flagged_df__(self):
    for metric in self.metrics_plan:
      flag = "flag_{}".format(metric)
      
      if not metric == "has_num_rows_greater_than":
        self.flags.append(flag)
      
      if self.__build_metric_expression__(metric):
        self.df = self.df.withColumn(flag, expr(self.__build_metric_expression__(metric)))
    
  
  def __show_report__(self, data_volume_error=False):
    execution_time = datetime.strftime(datetime.today(), "%Y-%m-%d %H:%M:%S")
    
    report_df = self.dirty_df \
      .withColumn("execution_time", lit(execution_time)) \
      .groupBy("execution_time") \
      .agg(
        *[sum(flag).alias("total_{}".format(flag.replace("flag_", ""))) for flag in (self.flags + ["flag_dirty_record"])]
      )
    
    try:
      report = report_df.collect()[0]

      print("##### Validation Report - {} #####".format(report["execution_time"]))
      print("##### Metrics Overhaul Result - {} total dirty records found #####".format(report["total_dirty_record"]))

      for flag in self.flags:
        metric_flag = flag.replace("flag_", "")
        total_flag = "total_{}".format(flag.replace("flag_", ""))

        if report[total_flag] > 0:
          print(self.metrics_base_report_text[metric_flag].format(report[total_flag]))
    except:
      print("##### Validation Report - {} #####".format(execution_time))
      print("##### Metrics Overhaul Result - {} total dirty records found #####".format(0))
      
    if data_volume_error:
      print(self.metrics_base_report_text["has_num_rows_greater_than"].format(self.metrics_plan["has_num_rows_greater_than"]))
      
          
  def run(self):
    self.__build_flagged_df__()
    
    flag_dirty_record = " + ".join(self.flags)
    
    self.df = self.df.withColumn("flag_dirty_record", expr(flag_dirty_record))
    
    self.dirty_df = self.df.filter(col("flag_dirty_record") == 1)
    self.clean_df = self.df.filter(col("flag_dirty_record") == 0).drop(*(self.flags + ["flag_dirty_record"]))
    
    if not self.metrics_plan.get("has_num_rows_greater_than", None):
      self.hasNumRowsGreaterThan()
    
    if self.clean_df.limit(self.metrics_plan["has_num_rows_greater_than"]).count() < self.metrics_plan["has_num_rows_greater_than"]:
      self.__show_report__(data_volume_error=True)
    else:
      self.__show_report__()
    
    return self.dirty_df, self.clean_df
