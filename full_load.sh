#!/bin/bash
#start= $1
#echo Data inicial = $1 
end=$(date '+%Y%m%d')

start=$(date -d $1 +%Y%m%d)
echo Data inicial = $1 
#end=$(date -d $end +%Y%m%d)

while [[ $start -le $end ]]
do
        
        #Baixando o arquivo .ZIP no diretório de destino
        wget ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP -P ZIP_FILES/$(date -d $start +%Y%m)
        
        #Extraindo somente arquivos necessários para um diretório específico
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbAtividadeProfissional$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbCargaHorariaSus$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbDadosProfissionalSus$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbEstabelecimento$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP rlEstabServClass$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbMunicipio$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m) 
        unzip ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP tbClassificacaoServico$(date -d $start +%Y%m).csv -d CSV_FILES/$(date -d $start +%Y%m)       
        
        #Removendo arquivos zipados
        #rm  ZIP_FILES/$(date -d $start +%Y%m)/BASE_DE_DADOS_CNES_$(date -d $start +%Y%m).ZIP

	#Incrementando variável de data
        start=$(date -d"$start + 1 month" +"%Y%m%d")

done

