#!/bin/bash
set -e
set -u

cd /wof
date -u
mkdir -p /wof/log
rm -rf /wof/log/joblog00


# install postgis functions;
wget https://raw.githubusercontent.com/CartoDB/cartodb-postgresql/master/scripts-available/CDB_TransformToWebmercator.sql
psql -f CDB_TransformToWebmercator.sql


# Parallel downloading
time parallel --results /wof/log/joblog00 -k  < /wof/code/parallel_joblist_00_download.sh

echo "---------------"
echo "### Directory sizes: "
du -sh *

echo "---------------"
echo "### Finished:"
date -u

echo "========== END OF job_download_inputs.sh log ============== "
