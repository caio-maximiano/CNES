#!/usr/bin/env python

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
        .builder \
        .appName("teste2") \
        .getOrCreate()


#Atribuindo a data atual no formato YYYYMM à variável ano_mes
ts=spark.sql(""" select current_timestamp() as ctime """).collect()[0]["ctime"]
#ano_mes = ts.strftime("%Y%m")
ano_mes = 202205

print(f"salario_data_{ano_mes}")
tbCargaHorariaSus = spark.read.csv("/Documents/PFC/CSV_FILES/{ano_mes}/tbCargaHorariaSus{ano_mes}")
#df_teste = spark.read.csv("/home/caio/Documents/iics_input/salario_data.csv")
#df_teste.write.csv("/home/caio/Documents/iics_input/saida_1.csv")
