
create or replace view   wd.wd_claims as
select
     wd_id
     
    ,a_wof_type

    ,is_cebuano(data)                       as wd_is_cebuano
    ,get_wdc_globecoordinate(data,'P625')   as p625_coordinate_location    

    ,get_wdc_item_label(data,'P31')    as p31_instance_of
    ,get_wdc_item_label(data,'P279')   as p279_subclass_of    
    ,get_wdc_item_label(data,'P17')    as p17_country_id     
    ,get_wdc_item_label(data,'P36')    as p36_capital
    ,get_wdc_item_label(data,'P1376')  as p1376_capital_of
    ,get_wdc_item_label(data,'P37')    as p37_official_language
    ,get_wdc_item_label(data,'P47')    as p47_shares_border_with
    ,get_wdc_item_label(data,'P131')   as p131_located_in_admin
    ,get_wdc_item_label(data,'P150')   as p131_contains_admin    
    ,get_wdc_item_label(data,'P206')   as p206_located_next_to_water   
    ,get_wdc_item_label(data,'P138')   as p138_name_after
    ,get_wdc_item_label(data,'P421')   as p421_timezone
    ,get_wdc_item_label(data,'P501')   as p501_enclave_within
    ,get_wdc_item_label(data,'P190')   as p190_sister_city
    ,get_wdc_item_label(data,'P460')   as p460_same_as
    ,get_wdc_item_label(data,'P30')    as p30_continent
    ,get_wdc_item_label(data,'P155')   as p155_follows
    ,get_wdc_item_label(data,'P159')   as p159_headquarters_location
    ,get_wdc_item_label(data,'P238')   as p238_iata_airport
    ,get_wdc_item_label(data,'P2959')  as p2959_permanent_dupl

    ,get_wdc_population(data, 'P1082') as p1082_population 

    ,get_wdc_value(data, 'P227')    as p227_gnd_id
    ,get_wdc_value(data, 'P297')    as p297_iso3166_1_alpha2
    ,get_wdc_value(data, 'P298')    as p298_iso3166_1_alpha3    
    ,get_wdc_value(data, 'P299')    as p299_iso3166_1_numeric
    ,get_wdc_value(data, 'P300')    as p300_iso3166_2
    ,get_wdc_value(data, 'P901')    as p901_fips10_4
    ,get_wdc_value(data, 'P214')    as p214_viaf
    ,get_wdc_value(data, 'P1997')   as p1997_facebook_places
    ,get_wdc_value(data, 'P646')    as p646_freebase
    ,get_wdc_value(data, 'P3417')   as p3417_quora_topic
    ,get_wdc_value(data, 'P1566')   as p1566_geonames
    ,get_wdc_value(data, 'P1667')   as p1667_tgn    
    ,get_wdc_value(data, 'P268')    as p268_bnf
    ,get_wdc_value(data, 'P349')    as p349_ndl  
    ,get_wdc_value(data, 'P213')    as p213_isni  
    ,get_wdc_value(data, 'P269')    as p269_sudoc               
    ,get_wdc_value(data, 'P402')    as p402_osm_rel    
    ,get_wdc_value(data, 'P244')    as p244_loc_gov
    ,get_wdc_value(data, 'P982')    as p982_musicbrainz_area
    ,get_wdc_value(data, 'P605')    as p605_nuts
    ,get_wdc_value(data, 'P409')    as p409_nla   
    ,get_wdc_value(data, 'P902')    as p902_hds     
    ,get_wdc_value(data, 'P949')    as p949_nl_israel    
    ,get_wdc_value(data, 'P3984')   as p3984_subredit
    ,get_wdc_value(data, 'P3911')   as p3911_stw_thesaurus    
    ,get_wdc_value(data, 'P2163')   as p2163_fast
    ,get_wdc_value(data, 'P1281')   as p1281_woeid    
    ,get_wdc_value(data, 'P2347')   as p2347_yso    
    ,get_wdc_value(data, 'P1670')   as p1560_lac       
    ,get_wdc_value(data, 'P2572')   as p2572_twitter_hastag  

    ,get_wdc_value(data, 'P18')     as p18_image
    ,get_claims_amount(data, 'P2044')   as p2044_elevation

    ,get_wdc_value(data, 'P443')    as p443_pronunciation_audio
    ,get_wdc_value(data, 'P898')    as p898_ipa_transcription
    ,get_wdc_value(data, 'P281')    as p281_postal_code
    ,get_wdc_value(data, 'P856')    as p856_official_website
    ,get_wdc_value(data, 'P1581')   as p1581_official_blog    
    ,get_wdc_value(data, 'P242')    as p242_locator_map_image
    ,get_wdc_value(data, 'P94')     as p94_coat_of_arms_image
    ,get_wdc_value(data, 'P41')     as p41_flag_image
    ,get_wdc_value(data, 'P935')    as p935_commons_gallery

    ,get_wdc_item_label(data, 'P1151')   as p111_main_wikimedia_portal   
    ,get_wdc_value(data, 'P473')    as p473_local_dialing_code

    ,get_wdc_monolingualtext(data, 'P1813')   as p1813_short_name
    ,get_wdc_monolingualtext(data, 'P1549')   as p1549_demonym
    ,get_wdc_monolingualtext(data, 'P1448')   as p1448_official_name
    ,get_wdc_monolingualtext(data, 'P1705')   as p1705_native_label
    ,get_wdc_monolingualtext(data, 'P1449')   as p1449_nick_name

    ,get_wdc_date(data, 'P580')    as p580_start_time
    ,get_wdc_date(data, 'P582')    as p582_end_time
    ,get_wdc_date(data, 'P571')    as p571_incepion_date
    ,get_wdc_date(data, 'P576')    as p576_dissolved_date

FROM wd.wdx

;

--  Todo:  https://www.wikidata.org/wiki/Q79791 Reconquista (Q79791)  ...
