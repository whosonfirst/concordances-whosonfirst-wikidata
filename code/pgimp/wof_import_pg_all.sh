#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

#
# cd /wof && ./code/pgimp/wof_import_pg_all.sh

mkdir -p /wof/code/pgimp/log/
echo "DROP TABLE IF EXISTS wofadmin.wofdata CASCADE;" | psql -e


function is_sqltable() {
  psql -c "\dt ${1}.*" | grep -wq $2
}

function pgimp_process {
    # check partition exists?
    if is_sqltable wofadmin wofdata_${1}
    then
        echo "wofadmin.wofdata_${1} exist .. skip"
    else
        echo "-------------------------------- $1 ------------------------------------------------ "
        rm -f /wof/code/pgimp/log/pgimp_${1}.log
        /wof/code/pgimp/pg_import.sh $1  >> /wof/code/pgimp/log/pgimp_${1}.log 2>&1
    fi
}

export -f is_sqltable
export -f pgimp_process
cat /wof/whosonfirst-data/isolist.csv | parallel -P $(nproc) pgimp_process {}

echo " -- OK : whosonfirst-data imported ---"

