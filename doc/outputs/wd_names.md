# wd_names  (wikidata labels -> wof names )

Table for exporting wikidata labels as a (wof) "name:???_x_preferred" 

### TODO
- [x] clean `wof_value`     "Jandaíra, Rio Grande do Norte"  ->  "Jandaíra"
- [x] remove cebuano values !
- [x] temporary - filter out long language codes- like (`zh-hans`)
- [ ] final fix for long language codes


### filenames

Filenames | expected size | description |
--------------------------| -----|  --- |
`wd.wd_names_preferred_validated.csv` | 923M| csv export |
`wd.wd_names_preferred_validated.db`  |	1.0G| sqlite3 format (csv export - imported to sqlite3) |
`wd.wd_names_preferred_validated.dbstruct.txt` |219| struct of the `wd.wd_names_preferred_validated.db` |



###  current  csv/sqlite3  structure 

n|variable|type|description|
-|--------|----|------|
0|id|TEXT|  wof id
1|metatable|TEXT| wof type
2|wof_name|TEXT| wof name
3|wof_country|TEXT| wof country
4|_matching_category|TEXT| internal: matching category
5|wd_id|TEXT|  wikidata id
6|wd_lang|TEXT|  wikidata language code
7|wof_lang|TEXT
8|wof_property|TEXT| wof property name - like: `name:hun_x_preferred`   
9|wof_value|TEXT| wof property value -

