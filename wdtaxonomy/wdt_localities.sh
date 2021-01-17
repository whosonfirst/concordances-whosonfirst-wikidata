#!/bin/bash
set -o errexit
source ./wdt_func.sh


wdtclr localities
wdtadd localities Q486972    "human settlement"

#-- added via manually debugging ---- most of them fixed, so you will find multiple times
wdtadd localities Q19610511  "Township"
wdtadd localities Q1867183   "local government area of Australia"
wdtadd localities Q1906268   "municipality of Bulgaria"
wdtadd localities Q1639634   "local government area of Nigeria"
wdtadd localities Q3076994   "commune of Cameroon"
wdtadd localities Q27676416  "city or town - type of local municipality in Quebec (Quebec don’t make the difference between city or town)"
wdtadd localities Q783930    "municipalities and cities of Serbia"
wdtadd localities Q192287    "administrative territorial entity of Russia"
wdtadd localities Q3750285   "administrative territorial entity of Canada"
wdtadd localities Q20732236  "administrative territorial entity of Trinidad and Tobago"
wdtadd localities Q2008050   "municipality of Albania"
wdtadd localities Q20724701  "city or town in Armenia"
wdtadd localities Q3685430   "municipality of Armenia"
wdtadd localities Q749622    "Antarctic research station"
wdtadd localities Q2706302   "municipality of Bosnia and Herzegovina"
wdtadd localities Q57058     "municipality of Croatia"
wdtadd localities Q17268368  "municipality of the Federation of Bosnia and Herzegovina"
wdtadd localities Q1906268   "municipality of Bulgaria"
wdtadd localities Q15630849  "village of Bulgaria"
wdtadd localities Q1780506   "commune of Benin"
wdtadd localities Q3184121   "municipality of Brazil"
wdtadd localities Q15210668  "lower-tier municipality"
wdtadd localities Q155239    "Indian reserve in Canada"
wdtadd localities Q27676420  "village municipality of Quebec"
wdtadd localities Q27676428  "municipality"
wdtadd localities Q6644703   "village in British Columbia"
wdtadd localities Q6644696   "village in Alberta"
wdtadd localities Q17366755  "hamlet in Alberta"
wdtadd localities Q6593035   "separated municipality in Ontario"
wdtadd localities Q14762300  "single-tier municipality"
wdtadd localities Q14762205  "municipal district of Alberta"
wdtadd localities Q27676422  "township municipality"
wdtadd localities Q3327874   "rural municipality of Canada"
wdtadd localities Q956318    "designated place of Canada"
wdtadd localities Q27676420  "village municipality of Quebec"
wdtadd localities Q27676524  "parish municipality"
wdtadd localities Q3518810   "unorganized area of Canada ( geographic region in Canada not part of a municipality or Indian reserve )"
wdtadd localities Q6616960   "district municipality in British Columbia"
wdtadd localities Q15731904  "municipal district of Nova Scotia"
wdtadd localities Q17305746  "Northern settlement of Saskatchewan"
wdtadd localities Q3327873   "local municipality of Quebec"
wdtadd localities Q5532181   "General Service Area"
wdtadd localities Q2679157   "commune of Ivory Coast"
wdtadd localities Q5172823   "corregimiento of Colombia"
wdtadd localities Q2997887   "corregimiento departamental of Colombia"
wdtadd localities Q5780443   "comuna of Colombia"
wdtadd localities Q2292572   "district of Costa Rica"
wdtadd localities Q16739079  "municipality of Cyprus"
wdtadd localities Q2602693   "municipality of Honduras"
wdtadd localities Q2225692   "fourth level administrative division in Indonesia"
wdtadd localities Q965568    "kelurahan , type of village or group of villages in Indonesia"
wdtadd localities Q2151232   "townland"
wdtadd localities Q1288520   "local council in Israel"
wdtadd localities Q16861602  "municipal council"
wdtadd localities Q3327862   "urban commune of Morocco"
wdtadd localities Q4229812   "commune of Moldova"
wdtadd localities Q2919801   "municipality of Luxembourg"
wdtadd localities Q1363145   "municipality of Lithuania"
wdtadd localities Q646793    "municipality of the Republic of Macedonia"
wdtadd localities Q7830262   "township of Burma"
wdtadd localities Q2989470   "commune of Mauritania"
wdtadd localities Q34986717  "city of Malaysia"
wdtadd localities Q605291    "municipality of Niger"
wdtadd localities Q1530705   "village development committee of Nepal"
wdtadd localities Q941036    "territorial authority of New Zealand"
wdtadd localities Q3685463   "corregimiento"
wdtadd localities Q1147395   "district of Turkey"
wdtadd localities Q12039044  "urban township of Taiwan"
wdtadd localities Q713146    "county-controlled city"
wdtadd localities Q2225003   "special municipality"
wdtadd localities Q42523     "atoll"   
wdtadd localities Q1077097   "tambon - central government unit in Thailand"

# check ...
wdtcopy localities Q618123    "geographical object"
