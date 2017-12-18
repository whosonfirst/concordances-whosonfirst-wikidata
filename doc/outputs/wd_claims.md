# wd_claims  (wikidata properties)

Table for exporting wikidata claims to WOF


### filenames

Filenames | expected size | description |
--------------------------| -----|  --- |
`wd.wd_claims_validated.csv`	| 	199M	| csv export |
`wd.wd_claims_validated.db`	  |	214M	  | sqlite3 format (csv export - imported to sqlite3) |
`wd.wd_claims_validated.dbstruct.txt`	|	2.3K | struct of the `wd.wd_claims_validated.db` |

### name convention


variables:   [WikidataPropertyId] + short abbreviation

examples
* p17_country_id  = P17-> [https://www.wikidata.org/wiki/Property:P17](https://www.wikidata.org/wiki/Property:P17)  
   * country ; sovereign state of this item
* p1566_geonames = P1566-> [https://www.wikidata.org/wiki/Property:P1566](https://www.wikidata.org/wiki/Property:P1566)  
   * GeoNames ID ; identifier in the GeoNames geographical database


### program code

* [https://github.com/ImreSamu/wof-wiki-dw/blob/master/code/75_wd_claims.sql](https://github.com/ImreSamu/wof-wiki-dw/blob/master/code/75_wd_claims.sql)


Mapping types - used in the `75_wd_claims.sql` - for wikidata types:

* get_wdc_globecoordinate() 
    * `[{"latitude": "49.096666666667", "longitude": "2.0408333333333"}, {"latitude": "49.096628", "longitude": "2.040722"}]`
    * `[{"latitude": "22.375833", "longitude": "31.611667"}]`

* get_wdc_item_label()
    * `[{"Q515": "city"}, {"Q2264924": "port city"}, {"Q1202812": "region of Djibouti"}]`
    * `[{"Q5119": "capital"}, {"Q515": "city"}, {"Q868893": "department of the Republic of the Congo"}]`
    * `[{"Q515": "city"}]`
    * `[{"Q9676": "Isle of Man"}, {"Q145": "United Kingdom"}]` 
    * `[{"Q2001266": "Tlyarata, Tlyaratinsky District, Republic of Dagestan"}]`
    * `[{"Q101418": "Lobamba"}, {"Q3904": "Mbabane"}]`

* get_wdc_population() - json array of 
    * `[{"population": "+993", "determination": "Q855531", "population_time": "+2011-00-00T00:00:00Z", "determinationlabel": "United Kingdom Census 2011"}]`
    * `[{"population": "+123867", "determination": "Q855531", "population_time": "+2011-00-00T00:00:00Z", "determinationlabel": "United Kingdom Census 2011"}, {"population": "+108863", "determination": "Q609443", "population_time": "+2001-00-00T00:00:00Z", "determinationlabel": "United Kingdom Census 2001"}, {"population": "+107496", "determination": "Q7887926", "population_time": "+1991-00-00T00:00:00Z", "determinationlabel": "United Kingdom Census 1991"}, {"population": "+87209", "determination": "Q21274970", "population_time": "+1981-00-00T00:00:00Z", "determinationlabel": "United Kingdom Census 1981"}, {"population": "+99168", "determination": "Q21274971", "population_time": "+1971-00-00T00:00:00Z", "determinationlabel": "United Kingdom Census 1971"}][{"population": "+123867", "determination": "Q855531", "population_time": "+2011-00-00T00:00:00Z", "determinationlabel": "United Kingdom Census 2011"}, {"population": "+108863", "determination": "Q609443", "population_time": "+2001-00-00T00:00:00Z", "determinationlabel": "United Kingdom Census 2001"}, {"population": "+107496", "determination": "Q7887926", "population_time": "+1991-00-00T00:00:00Z", "determinationlabel": "United Kingdom Census 1991"}, {"population": "+87209", "determination": "Q21274970", "population_time": "+1981-00-00T00:00:00Z", "determinationlabel": "United Kingdom Census 1981"}, {"population": "+99168", "determination": "Q21274971", "population_time": "+1971-00-00T00:00:00Z", "determinationlabel": "United Kingdom Census 1971"}]`

* get_wdc_value()   - array of text
    * `["GB-CHE"]`
    * `["4117851-8"]`
    * `["3220833", "2895992", "6556096"]`

* get_claims_amount()  - array 
    * `["+426"]`    -- elevation
    * `["+3220", "+3230"]`
    * `["+374", "+376"]`

* get_wdc_monolingualtext()
    * `[{"de": "Großhöflein (Burgenland)"}]`
    * `[{"en": "Butuan Airport"}, {"en": "Paliparan ng Butuan"}]`     <-- 2 english name! 
    * `[{"fr": "Aéroport de Bâle-Mulhouse-Fribourg"}, {"de": "Flughafen Basel-Mülhausen-Freiburg"}]`
    * `[{"hi": "अलीगढ़"}, {"ur": "علی گڑھ"}, {"en": "Aligarh"}]`    

* get_wdc_date()   -- array of dates
    * `["+1639-00-00"]`
    * `["+2005-00-00"]`
    * `["+2008-00-00", "+2006-00-00"]`


###  current  csv/sqlite3  structure 

n|variable|type|description|
-|--------|----|------|
0|id|TEXT|  wof id
1|metatable|TEXT| wof type
2|wof_name|TEXT| wof name
3|wof_country|TEXT| wof country
4|_matching_category|TEXT| internal: matching category
5|wd_id|TEXT|  wikidata id
6|a_wof_type|TEXT| internal: wikidata matching type
7|wd_is_cebuano|TEXT| internal: cebuano 
8|p625_coordinate_location|TEXT|
9|p31_instance_of|TEXT|
10|p279_subclass_of|TEXT|
11|p17_country_id|TEXT|
12|p36_capital|TEXT|
13|p1376_capital_of|TEXT|
14|p37_official_language|TEXT|
15|p47_shares_border_with|TEXT|
16|p131_located_in_admin|TEXT|
17|p131_contains_admin|TEXT|
18|p206_located_next_to_water|TEXT|
19|p138_name_after|TEXT|
20|p421_timezone|TEXT|
21|p501_enclave_within|TEXT|
22|p190_sister_city|TEXT|
23|p460_same_as|TEXT|
24|p30_continent|TEXT|
25|p155_follows|TEXT|
26|p159_headquarters_location|TEXT|
27|p238_iata_airport|TEXT|
28|p2959_permanent_dupl|TEXT|
29|p1082_population|TEXT|
30|p227_gnd_id|TEXT|
31|p297_iso3166_1_alpha2|TEXT|
32|p298_iso3166_1_alpha3|TEXT|
33|p299_iso3166_1_numeric|TEXT|
34|p300_iso3166_2|TEXT|
35|p901_fips10_4|TEXT|
36|p214_viaf|TEXT|
37|p1997_facebook_places|TEXT|
38|p646_freebase|TEXT|
39|p3417_quora_topic|TEXT|
40|p1566_geonames|TEXT|
41|p1667_tgn|TEXT|
42|p268_bnf|TEXT|
43|p349_ndl|TEXT|
44|p213_isni|TEXT|
45|p269_sudoc|TEXT|
46|p402_osm_rel|TEXT|
47|p244_loc_gov|TEXT|
48|p982_musicbrainz_area|TEXT|
49|p605_nuts|TEXT|
50|p409_nla|TEXT|
51|p902_hds|TEXT|
52|p949_nl_israel|TEXT|
53|p3984_subredit|TEXT|
54|p3911_stw_thesaurus|TEXT|
55|p2163_fast|TEXT|
56|p1281_woeid|TEXT|
57|p2347_yso|TEXT|
58|p1560_lac|TEXT|
59|p2572_twitter_hastag|TEXT|
60|p18_image|TEXT|
61|p2044_elevation|TEXT|
62|p443_pronunciation_audio|TEXT|
63|p898_ipa_transcription|TEXT|
64|p281_postal_code|TEXT|
65|p856_official_website|TEXT|
66|p1581_official_blog|TEXT|
67|p242_locator_map_image|TEXT|
68|p94_coat_of_arms_image|TEXT|
69|p41_flag_image|TEXT|
70|p935_commons_gallery|TEXT|
71|p111_main_wikimedia_portal|TEXT|
72|p473_local_dialing_code|TEXT|
73|p1813_short_name|TEXT|
74|p1549_demonym|TEXT|
75|p1448_official_name|TEXT|
76|p1705_native_label|TEXT|
77|p1449_nick_name|TEXT|
78|p580_start_time|TEXT|
79|p582_end_time|TEXT|
80|p571_incepion_date|TEXT|
81|p576_dissolved_date|TEXT|