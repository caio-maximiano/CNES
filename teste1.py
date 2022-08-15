#!/usr/bin/env

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import psycopg2
#from functions_pfc import get_csv, write_curated_file, update_table

spark = SparkSession \
        .builder \
        .appName("PFC") \
        .getOrCreate()

ano_mes = int(sys.argv[1])

def get_csv(file_name, ano_mes):
    csv_path = f"/home/caio/Documents/PFC/CSV_FILES/{ano_mes}"
    try:
        df = spark.read\
                .option("header", "true")\
                .option("delimiter", ";")\
                .csv(f"{csv_path}/{file_name}{ano_mes}.csv")
        return df
    except:
        print("O erro", sys.exc_info()[0], "ocorreu.")

def write_curated_file(df, file_name, ano_mes):
    curated_path = f'/home/caio/Documents/PFC/CURATED_FILES/{file_name}_{ano_mes}'

    try:
        df.coalesce(1)\
                .write\
                .option("header","True")\
                .csv(f"{curated_path}")

        for partition in os.listdir(f"{curated_path}"):
            if partition.startswith("part-"):
                old_name = os.path.join(f"{curated_path}", partition)
                new_name = os.path.join(f"{curated_path}", f"{file_name}_{ano_mes}.csv")
                os.rename(old_name, new_name)
    
    except:
        print("O erro", sys.exc_info()[0], "ocorreu.")


def update_table(table_name, file_name, ano_mes):

    try:
        conn = psycopg2.connect(database="localdb",
                                user='postgres', password='root', 
                                host='localhost', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()

        new_file = f'/home/caio/Documents/PFC/CURATED_FILES/{file_name}_{ano_mes}/{file_name}_{ano_mes}.csv'

        truncate = f'TRUNCATE TABLE {table_name}'
        cursor.execute(truncate)
        
        update = f'''COPY {table_name} FROM '{new_file}' DELIMITER ',' CSV HEADER;'''
        cursor.execute(update)
        print(f"Table {table_name} updated")
    
    except (Exception, psycopg2.Error) as error:
        print("Error in update operation", error)

    finally:
        # closing database connection.
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

if __name__ == '__main__':

    tbCargaHorariaSus = get_csv('tbCargaHorariaSus', ano_mes)
    rlEstabServClass = get_csv('rlEstabServClass', ano_mes)
    tbAtividadeProfissional = get_csv('tbAtividadeProfissional', ano_mes)
    tbClassificacaoServico = get_csv('tbClassificacaoServico', ano_mes)
    tbDadosProfissionalSus = get_csv('tbDadosProfissionalSus', ano_mes)
    tbEstabelecimento = get_csv('tbEstabelecimento', ano_mes)
    tbMunicipio = get_csv('tbMunicipio', ano_mes)

    cond_sp = [tbEstabelecimento.CO_MUNICIPIO_GESTOR == tbMunicipio.CO_MUNICIPIO,
           tbEstabelecimento.CO_ESTADO_GESTOR == 35]

    cond_serv = [rlEstabServClass.CO_SERVICO == tbClassificacaoServico.CO_SERVICO_ESPECIALIZADO,
        rlEstabServClass.CO_CLASSIFICACAO == tbClassificacaoServico.CO_CLASSIFICACAO_SERVICO]

    tbCargaHorariaSus = tbCargaHorariaSus.withColumnRenamed("TO_CHAR(A.DT_ATUALIZACAO,'DD/MM/YYYY')","DATA_ATUALIZACAO")

    estab_munic = tbEstabelecimento.join(tbMunicipio, cond_sp)
            

    df_serv = rlEstabServClass\
            .join(tbClassificacaoServico, cond_serv)\
            .join(estab_munic, rlEstabServClass.CO_UNIDADE == estab_munic.CO_UNIDADE)\
            .select(rlEstabServClass.CO_UNIDADE,
                rlEstabServClass.CO_SERVICO,
                rlEstabServClass.CO_CLASSIFICACAO,
                tbClassificacaoServico.DS_CLASSIFICACAO_SERVICO)

    df_final = tbCargaHorariaSus\
    .join(tbAtividadeProfissional, tbCargaHorariaSus.CO_CBO ==tbAtividadeProfissional.CO_CBO)\
    .join(estab_munic, tbCargaHorariaSus.CO_UNIDADE == estab_munic.CO_UNIDADE)\
    .join(tbDadosProfissionalSus, tbCargaHorariaSus.CO_PROFISSIONAL_SUS == tbDadosProfissionalSus.CO_PROFISSIONAL_SUS)\
    .select(tbCargaHorariaSus.CO_UNIDADE,
            tbCargaHorariaSus.CO_PROFISSIONAL_SUS,
            tbDadosProfissionalSus.NO_PROFISSIONAL,
            tbCargaHorariaSus.CO_CBO,
            tbCargaHorariaSus.TP_SUS_NAO_SUS,            
            tbAtividadeProfissional.DS_ATIVIDADE_PROFISSIONAL,
            estab_munic.NO_FANTASIA,
            estab_munic.NO_BAIRRO,
            estab_munic.NO_MUNICIPIO,
            estab_munic.CO_SIGLA_ESTADO,
            estab_munic.CO_CEP,
            tbCargaHorariaSus.DATA_ATUALIZACAO
            )\
    .withColumn('DATA_INGESTAO', to_date(current_timestamp()))

    write_curated_file(df_final, 'curated_estabelecimentos', ano_mes)

    write_curated_file(df_serv, 'curated_servicos', ano_mes)

    update_table('BI_SERVICOS', 'curated_servicos', ano_mes)

    update_table('BI_CNES_DATA', 'curated_estabelecimentos', ano_mes)