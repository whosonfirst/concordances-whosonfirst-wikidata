# suggestions:  ADD/DEL/REP wikidata to the current WOF data

Result tables for suggesting wikidata ids.





### filenames

Filenames |  description |
--------------------------| ----  |
`wfwd.wof_notfound_list_del.csv.gz`     | wof id + old wikidataid  :  ` where _matching_category like 'Notfound:DEL%'` |
`wfwd.wof_notfound_list_del.db.gz`      |	csv - converted to sqlite3 |
`wfwd.wof_notfound_list_del.dbstruct.txt` |  columns |
`wfwd.wof_validated_suggested_list_ok_add.csv.gz`  | wof id + new wikidataid  : `where _matching_category like 'OK-ADD:%' `  |
`wfwd.wof_validated_suggested_list_ok_add.db.gz` | csv - converted to sqlite3  |
`wfwd.wof_validated_suggested_list_ok_add.dbstruct.txt` | columns|
`wfwd.wof_validated_suggested_list_ok_rep.csv.gz` | wof id + new wikidataid  : `where  _matching_category like 'OK-REP:%' `|
`wfwd.wof_validated_suggested_list_ok_rep.db.gz` | csv - converted to sqlite3  |
`wfwd.wof_validated_suggested_list_ok_rep.dbstruct.txt` | columns|


### deleting - `wfwd.wof_notfound_list_del.csv.gz` 

Contains probably incorrect/changed wikidataids(`wof_wd_id`) with wof(`id`)  

Probably also need removing to current wikipedia pages ( "wof:concordances"."wk:page":  )

n|variable|type|description|
-|--------|----|------|
0|id|TEXT|  wof id
1|metatable|TEXT| wof type
2|wof_name|TEXT| wof name
3|wof_country|TEXT| wof country
4|wof_wd_id| TEXT |- current wikidata id
5|_matching_category|TEXT| internal: matching category
6|a_wof_type|TEXT| internal: wikidata matching type


### add - `wfwd.wof_validated_suggested_list_ok_add.csv.gz` 

proposed new Wikidata -ids.   ( add `wd_id` to (wof)`id`)

n|variable|type|description|
-|--------|----|------|
0|id|TEXT|  wof id
1|metatable|TEXT| wof type
2|wof_name|TEXT| wof name
3|wof_country|TEXT| wof country
4|_matching_category|TEXT| internal: matching category
5|wd_id|TEXT|  wikidata id
6|a_wof_type|TEXT| internal: wikidata matching type


### replace - `wfwd.wof_validated_suggested_list_ok_rep.csv.gz`

proposed UPDATE to the Wikidata -ids.   ( update to `wd_id` to (wof)`id`)

Probably also need removing to current wikipedia pages ( "wof:concordances"."wk:page":  )

n|variable|type|description|
-|--------|----|------|
0|id|TEXT|  wof id
1|metatable|TEXT| wof type
2|wof_name|TEXT| wof name
3|wof_country|TEXT| wof country
4|_matching_category|TEXT| internal: matching category
5|wd_id|TEXT|  wikidata id
6|a_wof_type|TEXT| internal: wikidata matching type

