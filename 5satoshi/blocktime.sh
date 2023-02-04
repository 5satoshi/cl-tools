
wget https://loyce.club/blockdata/blockdata.lastday.txt.gz 
zcat blockdata.txt.gz | cut -d, -f1,3 > blocktime.txt

export GOOGLE_APPLICATION_CREDENTIALS=/home/honc/.keys/key.json;

bq load --autodetect --source_format=CSV version_1.blocktime blocktime.txt
