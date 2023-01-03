from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dateutil.relativedelta import *
import zipfile, sys, os, psycopg2, wget
import datetime

spark = SparkSession \
        .builder \
        .appName("PFC") \
        .getOrCreate()

csv_path = "C:/Users/013503631/Documents/CNES_SUS-jupyter/CSV_FILES/"
zip_path = "C:/Users/013503631/Documents/CNES_SUS-jupyter/ZIP_FILES/"
curated_path = "C:/Users/013503631/Documents/CNES_SUS-jupyter/CURATED_FILES/"

use_date = datetime.datetime.now() + relativedelta(months=-3)
ano_mes = use_date.strftime("%Y%m")

def extract_zip(periodo):
    
    ftp_path = f"ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_{ano_mes}.ZIP"
    
    #Verifica se arquivo já não existe para iniciar downlaod do servidor FTP
    if os.path.exists(zip_path + f"BASE_DE_DADOS_CNES_{ano_mes}.ZIP") == False: 
        try:
            wget.download(ftp_path, zip_path) #Acessa servidor FTP para baixar arquivo
            print(f"Dados do período {ano_mes} extraídos")
        except:
            print("O erro", sys.exc_info()[0], "ocorreu.")
    else:
        print(f"O arquivos zip de {ano_mes} já existe")
        
def extract_csv(periodo):
    
    final_path = f"{csv_path}{periodo}"
    
    #Verifica se a pasta destino não existe para iniciar processo
    if os.path.exists(final_path) == False:
        try:
            os.chdir(zip_path)
            for file in os.listdir(zip_path):
                if zipfile.is_zipfile(file): 
                    with zipfile.ZipFile(file) as item: 
                       item.extractall(final_path)
                    os.remove(file)
        except:
            print("O erro", sys.exc_info()[0], "ocorreu.")     
    else:
        print(f"A pasta de destino {ano_mes} já existe")       
        
def get_csv(file_name, periodo):
    
    file_path = f"{csv_path}{periodo}/{file_name}{periodo}.csv"
    
    try:
        df = spark.read\
                .option("header", "true")\
                .option("delimiter", ";")\
                .csv(f"{file_path}")
        return df
    except:
        print("O erro", sys.exc_info()[0], "ocorreu.")


def write_curated_file(df, file_name, ano_mes):
    file_path = f'{curated_path}{file_name}_{ano_mes}'

    try:
        df.coalesce(1)\
                .write\
                .mode("overwrite")\
                .option("header","True")\
                .csv(f"{file_path}")

        for partition in os.listdir(f"{file_path}"):
            if partition.startswith("part-"):
                old_name = os.path.join(f"{file_path}", partition)
                new_name = os.path.join(f"{file_path}", f"{file_name}_{ano_mes}.csv")
                os.rename(old_name, new_name)
    
    except:
        print("O erro", sys.exc_info()[0], "ocorreu.")


def update_table(table_name, file_name, ano_mes):

    try:
        conn = psycopg2.connect(database="postgres",
                                user='postgres', password='system', 
                                host='localhost', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()

        new_file = f'C:/Users/013503631/Documents/CNES_SUS-jupyter/CURATED_FILES/{file_name}_{ano_mes}/{file_name}_{ano_mes}.csv'

        truncate = f'TRUNCATE TABLE {table_name}'
        cursor.execute(truncate)
        
        update = f'''COPY {table_name} FROM '{new_file}' DELIMITER ',' CSV HEADER;'''
        cursor.execute(update)
        print(f"Tabela {table_name} atualizada com dados de {ano_mes}")
    
    except (Exception, psycopg2.Error) as error:
        print("Error in update operation", error)

    finally:
        # closing database connection.
        if conn:
            cursor.close()
            conn.close()
            print("Conexão PostgreSQL encerrada")

extract_zip(ano_mes)

extract_csv(ano_mes)

tbCargaHorariaSus = get_csv('tbCargaHorariaSus', ano_mes)
rlEstabServClass = get_csv('rlEstabServClass', ano_mes)
tbAtividadeProfissional = get_csv('tbAtividadeProfissional', ano_mes)
tbClassificacaoServico = get_csv('tbClassificacaoServico', ano_mes)
tbDadosProfissionalSus = get_csv('tbDadosProfissionalSus', ano_mes)
tbEstabelecimento = get_csv('tbEstabelecimento', ano_mes)
tbMunicipio = get_csv('tbMunicipio', ano_mes)

cond_sp = [tbEstabelecimento.CO_MUNICIPIO_GESTOR == tbMunicipio.CO_MUNICIPIO,
           tbEstabelecimento.CO_ESTADO_GESTOR == 35] #SP

cond_serv = [rlEstabServClass.CO_SERVICO == tbClassificacaoServico.CO_SERVICO_ESPECIALIZADO,
        rlEstabServClass.CO_CLASSIFICACAO == tbClassificacaoServico.CO_CLASSIFICACAO_SERVICO]

estab_munic = tbEstabelecimento.join(tbMunicipio, cond_sp)


df_serv = rlEstabServClass\
            .join(tbClassificacaoServico, cond_serv)\
            .join(estab_munic, rlEstabServClass.CO_UNIDADE == estab_munic.CO_UNIDADE)\
            .select(rlEstabServClass.CO_UNIDADE,
                rlEstabServClass.CO_SERVICO,
                rlEstabServClass.CO_CLASSIFICACAO,
                tbClassificacaoServico.DS_CLASSIFICACAO_SERVICO)\
            .withColumn('DATA_INGESTAO', to_date(current_timestamp()))

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
            estab_munic.CO_MUNICIPIO,
            estab_munic.CO_SIGLA_ESTADO,
            estab_munic.CO_CEP,
            )\
    .withColumn('DATA_INGESTAO', to_date(current_timestamp()))

write_curated_file(df_final, 'curated_estabelecimentos', ano_mes)

write_curated_file(df_serv, 'curated_servicos', ano_mes)

update_table('curated_servicos', 'curated_servicos', ano_mes)

update_table('curated_estabelecimentos', 'curated_estabelecimentos', ano_mes)