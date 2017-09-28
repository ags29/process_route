from pyspark.sql.functions import randn, lit, col
from pyspark.sql.functions import col
from pyspark.sql import functions as f
import zipfile
from pyspark.sql import Row
import re
import io
from pyspark.sql import types as tp
from pyspark.sql.types import StructField, StructType
from itertools import chain
import pandas as pd
from pyspark.sql import DataFrame


class ProcessRoute(object):
  def __init__(raw_path, schema_path, num_partitons):
    self.raw_path=raw_path
    self.schema_path=schema_path
    self.num_partitions=num_partitions
    
  # extract zip file
  def _zip_extract(self, x):
      data=[]
      for elt in x:
        in_memory_data = io.BytesIO(elt[1])
        file_obj = zipfile.ZipFile(in_memory_data, "r")
        files = [i for i in file_obj.namelist()]
        for file in files:
          raw_data=file_obj.open(file).read()
          data.append([re.split('[|]',line) for index, line in enumerate(raw_data.split('\r\n')) if line!='' and index>=1])
      return list(chain(*data))
    
  # type conversions with error check-some fields have inconsistent entries
  def _convert_with_error_f(self, x):
    try:
      output=float(x)
    except:
      output=-99999.0
    return(output)
  
  def convert_with_error_i(x):
    try:
      output=int(x)
    except:
      output=-99999
    return(output)
  
  # process line
  def _process_raw_schema(self, raw):
    data=[]
    for line  in raw:
      for i in range(len(line)):
        if ps_schema[i]=='integer':
          line[i]=convert_with_error_i(line[i])
        if ps_schema[i]=='float':
          line[i]=convert_with_error_f(line[i])
      data.append(Row(**{v:line[i] for i,v in enumerate(s_vars)}))
    return(data)
    
  # partition by 
  def _repart(df):
    """
    Repartition frame id
    """
    df=df.map(lambda x: (x['FrameID'], x)).partitionBy(self.num_partition, lambda x: hash(str(x))%120).map(lambda x: x[1]).toDF()
    return(df)
    
  def process_file(self, file_num):
    if file_num not in range(1,8):
      raise ValueError("File number outside range 1-7")
    # particular cases
    if file_num==2:
      zips=sc.binaryFiles(self.raw_path+"/file%d/*"%(file_num))\
          .mapPartitions(zip_extract).map(lambda x: [self._convert_with_error_i(x[i]) for i in range(len(x)) ]).toDF()
          
    if file_num!=2:
      # Load schema
      schema=spark.read.csv(self.schema_path+"/File%d.csv"%(file_num), header=True).toPandas()
      varname=[col for col in schema.columns if 'ariable' in col][0]
      ps_schema=schema.sql_dtype.tolist()
      s_vars=[re.sub(' ', '', v) for v in schema[varname].tolist()]
      
      # Load zip, create DF and save as parquet in S3
      zips = sc.binaryFiles(self.raw_path+"/file%d/*"%(file_num))\
      .mapPartitions(lambda x: self._zip_extract(x))
      
      if i not in [1,4,7]:
        zips = zips.repartition(70).mapPartitions(self._process_raw_schema).toDF()
      else:
        zips=self._repart(zips).mapPartitions(self._process_raw_schema).toDF()

    zips.write.save(self.raw_path+"/file%d_test/"%(file_num), mode='overwrite')
    
