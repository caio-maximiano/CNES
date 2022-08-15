#!/bin/bash

mes=$(date -d "$1 - 1  month" +%Y%m%d)

#./get_data_ftp.sh $mes
python3 teste1.py $mes

