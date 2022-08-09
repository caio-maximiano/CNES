#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from soupsieve import select
import os, sys

spark = SparkSession \
        .builder \
        .appName("teste2") \
        .getOrCreate()

ano_mes = sys.argv[1]
#Atribuindo a data atual no formato YYYYMM à variável ano_mes
#ts=spark.sql(""" select current_timestamp() as ctime """).collect()[0]["ctime"]
#ano_mes = ts.strftime("%Y%m")

tbCargaHorariaSus = spark.read\
                    .option("header", "true")\
                    .option("delimiter", ";")\
                    .csv(f"/home/caio/Documents/PFC/CSV_FILES/{202205}/tbCargaHorariaSus{202205}.csv")

rlEstabServClass = spark.read\
                    .option("header", "true")\
                    .option("delimiter", ";")\
                    .csv(f"/home/caio/Documents/PFC/CSV_FILES/{202205}/rlEstabServClass{202205}.csv")

tbAtividadeProfissional = spark.read\
                    .option("header", "true")\
                    .option("delimiter", ";")\
                    .csv(f"/home/caio/Documents/PFC/CSV_FILES/{202205}/tbAtividadeProfissional{202205}.csv")

tbClassificacaoServico = spark.read\
                    .option("header", "true")\
                    .option("delimiter", ";")\
                    .csv(f"/home/caio/Documents/PFC/CSV_FILES/{202205}/tbClassificacaoServico{202205}.csv")

tbDadosProfissionalSus = spark.read\
                    .option("header", "true")\
                    .option("delimiter", ";")\
                    .csv(f"/home/caio/Documents/PFC/CSV_FILES/{202205}/tbDadosProfissionalSus{202205}.csv")

tbEstabelecimento = spark.read\
                    .option("header", "true")\
                    .option("delimiter", ";")\
                    .csv(f"/home/caio/Documents/PFC/CSV_FILES/{202205}/tbEstabelecimento{202205}.csv")

tbMunicipio = spark.read\
                    .option("header", "true")\
                    .option("delimiter", ";")\
                    .csv(f"/home/caio/Documents/PFC/CSV_FILES/{202205}/tbMunicipio{202205}.csv")

estab_sp = tbEstabelecimento\
    .filter(tbEstabelecimento.CO_ESTADO_GESTOR == 35)\
    .withColumnRenamed(tbEstabelecimento.columns[32],"DATA_ATUALIZACAO")

estab_joined = estab_sp\
        .join(rlEstabServClass, estab_sp.CO_UNIDADE == rlEstabServClass.CO_UNIDADE)\
        .join(tbMunicipio, estab_sp.CO_MUNICIPIO_GESTOR == tbMunicipio.CO_MUNICIPIO)\
        .select(estab_sp.CO_UNIDADE,
                estab_sp.NO_BAIRRO,
                estab_sp.CO_CNES, #verificar se precisa
                estab_sp.CO_CEP,
                estab_sp.NO_FANTASIA,
                estab_sp.CO_MUNICIPIO_GESTOR,
                estab_sp.DATA_ATUALIZACAO,
                tbMunicipio.NO_MUNICIPIO,
                tbMunicipio.CO_SIGLA_ESTADO,
                rlEstabServClass.CO_SERVICO,
                rlEstabServClass.CO_CLASSIFICACAO)

cond = [estab_joined.CO_SERVICO == tbClassificacaoServico.CO_SERVICO_ESPECIALIZADO,
        estab_joined.CO_CLASSIFICACAO == tbClassificacaoServico.CO_CLASSIFICACAO_SERVICO]

estab_final = estab_joined\
        .join(tbClassificacaoServico, cond)\
        .select(estab_joined.CO_UNIDADE,
                estab_joined.NO_FANTASIA,
                estab_joined.CO_CEP,
                estab_joined.NO_BAIRRO,
                estab_joined.CO_MUNICIPIO_GESTOR,
                estab_joined.NO_MUNICIPIO,
                estab_joined.CO_SIGLA_ESTADO,
                estab_joined.CO_SERVICO,
                estab_joined.CO_CLASSIFICACAO,
                tbClassificacaoServico.DS_CLASSIFICACAO_SERVICO,
                estab_joined.DATA_ATUALIZACAO)\
        .withColumn('DATA_INGESTAO', lit(ano_mes).cast('Integer'))
   
estab_final.repartition(1)\
            .write\
            .option("header","True")\
            .csv(f"/home/caio/Documents/PFC/CURATED_FILES/estab_final_{ano_mes}")

PATH = f"/home/caio/Documents/PFC/CURATED_FILES/estab_final_{ano_mes}/"

for filename in os.listdir(PATH):
    if filename.startswith("part-"):
        old_name = os.path.join(PATH, filename)
        new_name = os.path.join(PATH, f"{ano_mes}")
        os.rename(old_name, new_name)

import psycopg2
  
conn = psycopg2.connect(database="localdb",
                        user='postgres', password='root', 
                        host='localhost', port='5432')

conn.autocommit = True
cursor = conn.cursor()

sql = f'''COPY ESTAB(CO_UNIDADE,NO_FANTASIA ,CO_CEP,NO_BAIRRO ,CO_MUNICIPIO_GESTOR ,NO_MUNICIPIO ,\
    CO_SIGLA_ESTADO ,CO_SERVICO,CO_CLASSIFICACAO,DS_CLASSIFICACAO_SERVICO ,DATA_ATUALIZACAO, DATA_INGESTA)
FROM '/home/caio/Documents/PFC/CURATED_FILES/estab_final_{ano_mes}/{ano_mes}'
DELIMITER ','
CSV HEADER;'''

cursor.execute(sql)

sql2 = '''select * from ESTAB;'''
cursor.execute(sql2)
for i in cursor.fetchall():
    print(i)

conn.commit()
conn.close()