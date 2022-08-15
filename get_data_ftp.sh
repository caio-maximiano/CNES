#!/bin/bash

#Definição das variáveis
mes=$(date -d $1 +%Y%m%d)
echo Baixando arquivos com data referência "$mes".

#Baixando o arquivo .ZIP do servitor FTP no diretório de destino
wget ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_$(date -d $mes +%Y%m).ZIP -P ZIP_FILES/$(date -d $mes +%Y%m)

echo Extraindo arquivos com data referência "$mes".
#Extraindo somente arquivos necessários para um diretório temporário
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbAtividadeProfissional$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbCargaHorariaSus$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbDadosProfissionalSus$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbEstabelecimento$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP rlEstabServClass$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbMunicipio$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m) 
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbClassificacaoServico$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)
        
#Removendo arquivos zipados
rm  ZIP_FILES/$(date -d $mes +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $mes +%Y%m).ZIP




