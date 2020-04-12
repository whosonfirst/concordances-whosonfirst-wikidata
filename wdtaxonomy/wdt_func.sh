#!/bin/bash

function wdtadd {
 wdgrp=$1
 wd=$2

 echo "### $2, $3 : _wdadd_ " >> wikidata_${wdgrp}.csv
 wdtaxonomy -l en,es,de,fr,pt,it,da,nl,pl,cs,sk,ro,hu,sv,sl,no,fi,et,lt,lv,rm,ca,el,he,ru,uk,mk,be,hr,bg,ja,zh,ko,ar,ta,eo,fy,gu,tr,ltg,fa,bn,nb,ms,id,su,sw,eu,az,nn,nb,pt-br,th,zh-tw,zh-cn,sr,hy,gl,la,nds-nl,gan,lmo,tl,lb,kk,is,or,an,uz,sq,rue,pms,kv,koi,ast,ia,ky,af,hi,en-gb,nds,scn,te \
    -b -f csv $wd | cut -d',' -f2,3 | awk '!/(id,label)/' >> wikidata_${wdgrp}.csv

 echo "----- $wdgrp ----"
 cat   wikidata_${wdgrp}.csv
 cat   wikidata_${wdgrp}.csv | wc -l
 cat   wikidata_${wdgrp}.csv | grep ,\"\" || true

}



function wdtcopy {
 wdgrp=$1
 wd=$2
 wdlabel=$3
 echo "$2, $3 , _wdtcopy_ " >> wikidata_${wdgrp}.csv

 echo "----- $wdgrp ----"
 cat   wikidata_${wdgrp}.csv
 cat   wikidata_${wdgrp}.csv | wc -l
 cat   wikidata_${wdgrp}.csv | grep ,\"\" || true
}


function wdtclr {
 wdgrp=$1
 rm -f   wikidata_${wdgrp}.csv
}

