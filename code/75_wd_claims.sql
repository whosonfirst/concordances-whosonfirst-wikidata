


-- https://github.com/maxlath/wikidata-sdk/blob/master/docs/install.md
-- on truthyness: https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Truthy_statements
-- https://github.com/nichtich/wikidata-taxonomy


CREATE OR REPLACE FUNCTION public.get_claims2text(data jsonb, wdproperty text)
RETURNS text
IMMUTABLE
LANGUAGE sql
AS $$
    select  mainsnak_datavalue 
    from (
            SELECT
                 jsonb_array_elements( data->'claims'->wdproperty ) ->'mainsnak'->'datavalue'->>'value'  as mainsnak_datavalue
                ,jsonb_array_elements( data->'claims'->wdproperty )                          ->>'rank'   as rank
            FROM jsonb_each(data)
            ORDER BY rank desc
            LIMIT 1
        ) t
    where rank != 'deprecated'    ;
$$;


-- https://www.wikidata.org/wiki/Help:Dates
CREATE OR REPLACE FUNCTION public.get_claims_dates(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE
LANGUAGE sql
AS $$
    select  jsonb_agg( split_part(timevalue,'T',1)) 
    from (
            SELECT
                 jsonb_array_elements( data->'claims'->wdproperty ) ->'mainsnak'->'datavalue'->'value'->>'time'  as timevalue
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='preferred'
          UNION ALL
            SELECT
                 jsonb_array_elements( data->'claims'->wdproperty ) ->'mainsnak'->'datavalue'->'value'->>'time'  as timevalue
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='normal'
        ) t
    ;
$$;



CREATE OR REPLACE FUNCTION public.get_wdlabel(wdid text)
RETURNS jsonb
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT jsonb_build_object(wd_id, wd_label)          
    FROM wdlabels.en
    WHERE wd_id=wdid
    ;
$$;

CREATE OR REPLACE FUNCTION public.get_wdlabeltext(wdid text)
RETURNS text
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT wd_label          
    FROM wdlabels.en
    WHERE wd_id=wdid
    ;
$$;


-- [{"population": "+1436697", "determination": null, "population_time": "+2014-01-01T00:00:00Z", "determinationlabel": null}, {"population": "+1256810", "determination": null, "population_time": "+2005-00-00T00:00:00Z", "determinationlabel": null}, {"population": "+1492510", "determination": null, "population_time": "+2017-00-00T00:00:00Z", "determinationlabel": null}]
-- [{"population": "+1316", "determination": "Q15911027", "population_time": "+2017-01-01T00:00:00Z", "determinationlabel": "demographic balance"}]
-- [{"population": "+9244", "determination": "Q39825", "population_time": "+2010-00-00T00:00:00Z", "determinationlabel": "census"}]

CREATE OR REPLACE FUNCTION public.get_wdc_population_list(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE
LANGUAGE sql
AS $$
    select  jsonb_agg(pop) 
    from (
    
        select  *,claimorder
        FROM(
            SELECT
           	jsonb_build_object(
                  'population'  
                  ,wdp ->'mainsnak'->'datavalue'->'value'->>'amount'
                  ,'population_time' 
                  ,wdp ->'qualifiers'->'P585'->0->'datavalue'->'value'->>'time'
                  ,'determination' 
                  ,wdp ->'qualifiers'->'P459'->0->'datavalue'->'value'->>'id'
                  ,'determinationlabel' 
                  ,get_wdlabeltext( wdp ->'qualifiers'->'P459'->0->'datavalue'->'value'->>'id' )
                ) as pop
                ,1 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE  wdp->>'rank'='preferred'  -- and  wdp->'mainsnak'->'datavalue'->>'type' = 'globecoordinate'
          UNION ALL
            SELECT
           	jsonb_build_object(
                  'population'  
                  ,wdp ->'mainsnak'->'datavalue'->'value'->>'amount'
                  ,'population_time' 
                  ,wdp ->'qualifiers'->'P585'->0->'datavalue'->'value'->>'time'
                  ,'determination' 
                  ,wdp ->'qualifiers'->'P459'->0->'datavalue'->'value'->>'id'
                  ,'determinationlabel' 
                  ,get_wdlabeltext( wdp ->'qualifiers'->'P459'->0->'datavalue'->'value'->>'id' )
                ) as pop
                ,2 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='normal'  -- and  wdp->'mainsnak'->'datavalue'->>'type' = 'globecoordinate'
        ) s
        order BY claimorder     
    ) t
 
    ;
$$;

CREATE OR REPLACE FUNCTION public.get_claims2ajsonb_label(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE
LANGUAGE sql
AS $$
    select  jsonb_agg(get_wdlabel(mainsnak_datavalue)) 
    from (
            SELECT
                 wdp ->'mainsnak'->'datavalue'->'value'->>'id'  as mainsnak_datavalue
                ,wdp                          ->>'rank'   as rank
                ,wdp                          ->>'type'   as wtype                
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            -- ORDER BY rank desc
        ) t
    where rank != 'deprecated'    ;
$$;


CREATE OR REPLACE FUNCTION public.get_claims2ajsonb(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE
LANGUAGE sql
AS $$
    select  jsonb_agg(mainsnak_datavalue) 
    from (
            SELECT
                 wdp ->'mainsnak'->'datavalue'->'value'->>'id'  as mainsnak_datavalue
                ,wdp                          ->>'rank'   as rank
                ,wdp                          ->>'type'   as wtype                
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            -- ORDER BY rank desc
        ) t
    where rank != 'deprecated'    ;
$$;



CREATE OR REPLACE FUNCTION public.get_claims2jsonba(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE
LANGUAGE sql
AS $$
    select  jsonb_agg(mainsnak_datavalue) 
    from (
            SELECT
                 jsonb_array_elements( data->'claims'->wdproperty ) ->'mainsnak'->'datavalue'->>'value'  as mainsnak_datavalue
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='preferred'
          UNION ALL
            SELECT
                 jsonb_array_elements( data->'claims'->wdproperty ) ->'mainsnak'->'datavalue'->>'value'  as mainsnak_datavalue
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='normal'
        ) t
    ;
$$;

CREATE OR REPLACE FUNCTION public.get_monolingualtext_claims2ajsonb(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE
LANGUAGE sql
AS $$
    select  jsonb_agg(lang) 
    from (
            SELECT
                jsonb_build_object( 
                     wdp ->'mainsnak'->'datavalue'->'value'->>'language'  
                    ,wdp ->'mainsnak'->'datavalue'->'value'->>'text' 
                ) as lang
                ,wdp                          ->>'rank'   as rank
                ,wdp                          ->>'type'   as wtype                
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            -- ORDER BY rank desc
        ) t
    where rank != 'deprecated'    ;
$$;

CREATE OR REPLACE FUNCTION public.get_claims_amount(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE
LANGUAGE sql
AS $$
    select  jsonb_agg( amountvalue ) 
    from (
            SELECT jsonb_array_elements( data->'claims'->wdproperty ) ->'mainsnak'->'datavalue'->'value'->>'amount'  as amountvalue
                  ,1 as claimorder
            FROM   jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE  wdp->>'rank'='preferred'
          UNION ALL
            SELECT jsonb_array_elements( data->'claims'->wdproperty ) ->'mainsnak'->'datavalue'->'value'->>'amount'  as amountvalue
                   ,2 as claimorder
            FROM   jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE  wdp->>'rank'='normal'
        ) t
    order by claimorder    
    ;
$$;



-- [{"latitude": "-40.7", "longitude": "-66.15"}]
-- [{"latitude": "-32.6", "longitude": "-66.125"}]
-- [{"latitude": "-28.05", "longitude": "-58.233333333333"}, {"latitude": "-28.04532", "longitude": "-58.22835"}]

CREATE OR REPLACE FUNCTION public.get_wdc_globecoordinate(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE
LANGUAGE sql
AS $$
    select  jsonb_agg(coord) 
    from (
    
        select  *,claimorder
        FROM(
        
            SELECT
            	jsonb_build_object(
                  'latitude'  
                  ,wdp ->'mainsnak'->'datavalue'->'value'->>'latitude'
                  ,'longitude' 
                  ,wdp ->'mainsnak'->'datavalue'->'value'->>'longitude'
                ) as coord
                ,1 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE  wdp->>'rank'='preferred'  and  wdp->'mainsnak'->'datavalue'->>'type' = 'globecoordinate'
          UNION ALL
            SELECT
            	jsonb_build_object(
                  'latitude'  
                  ,wdp ->'mainsnak'->'datavalue'->'value'->>'latitude'
                  ,'longitude' 
                  ,wdp ->'mainsnak'->'datavalue'->'value'->>'longitude'
                ) as coord
                ,2 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='normal'  and  wdp->'mainsnak'->'datavalue'->>'type' = 'globecoordinate'
        ) s
        order BY claimorder     
    ) t
 
    ;
$$;



drop table if exists wikidata.wd_claims CASCADE;
create table wikidata.wd_claims as
select
     data->>'id'::text                      as wd_id

    ,get_wdc_globecoordinate(data,'P625')   as p625_coordinate location    

    ,get_claims2ajsonb_label(data,'P31')    as p31_instance_of
    ,get_claims2ajsonb_label(data,'P279')   as p279_subclass_of    
    ,get_claims2ajsonb_label(data,'P17')    as p17_country_id     
    ,get_claims2ajsonb_label(data,'P36')    as p36_capital
    ,get_claims2ajsonb_label(data,'P1376')  as p1376_capital_of
    ,get_claims2ajsonb_label(data,'P37')    as p37_official_language
    ,get_claims2ajsonb_label(data,'P47')    as p47_shares_border_with
    ,get_claims2ajsonb_label(data,'P131')   as p131_located_in_admin
    ,get_claims2ajsonb_label(data,'P150')   as p131_contains_admin    
    ,get_claims2ajsonb_label(data,'P206')   as p206_located_next_to_water   
    ,get_claims2ajsonb_label(data,'P138')   as p138_name_after
    ,get_claims2ajsonb_label(data,'P421')   as p421_timezone
    ,get_claims2ajsonb_label(data,'P501')   as p501_enclave_within
    ,get_claims2ajsonb_label(data,'P190')   as p190_sister_city
    ,get_claims2ajsonb_label(data,'P460')   as p460_same_as
    ,get_claims2ajsonb_label(data,'P30')    as p30_continent
    ,get_claims2ajsonb_label(data,'P155')   as p155_follows
    ,get_claims2ajsonb_label(data,'P159')   as p159_headquarters_location
    ,get_claims2ajsonb_label(data,'P238')   as p238_iata_airport

    ,get_wdc_population_list(data, 'P1082') as p1082_population 

    ,get_claims2jsonba(data, 'P227')    as p227_gnd_id
    ,get_claims2jsonba(data, 'P300')    as p300_iso3166_2
    ,get_claims2jsonba(data, 'P901')    as p901_fips10_4
    ,get_claims2jsonba(data, 'P214')    as p214_viaf
    ,get_claims2jsonba(data, 'P1997')   as p1997_facebook_places
    ,get_claims2jsonba(data, 'P646')    as p646_freebase
    ,get_claims2jsonba(data, 'P3417')   as p3417_quora_topic
    ,get_claims2jsonba(data, 'P1566')   as p1566_geonames
    ,get_claims2jsonba(data, 'P1667')   as p1667_tgn    
    ,get_claims2jsonba(data, 'P268')    as p268_bnf
    ,get_claims2jsonba(data, 'P349')    as p349_ndl  
    ,get_claims2jsonba(data, 'P213')    as p213_isni  
    ,get_claims2jsonba(data, 'P269')    as p269_sudoc               
    ,get_claims2jsonba(data, 'P402')    as p402_osm_rel    
    ,get_claims2jsonba(data, 'P244')    as p244_loc_gov
    ,get_claims2jsonba(data, 'P982')    as p982_musicbrainz_area
    ,get_claims2jsonba(data, 'P605')    as p605_nuts
    ,get_claims2jsonba(data, 'P409')    as p409_nla   
    ,get_claims2jsonba(data, 'P902')    as p902_hds     
    ,get_claims2jsonba(data, 'P949')    as p949_nl_israel    
    ,get_claims2jsonba(data, 'P3984')   as p3984_subredit
    ,get_claims2jsonba(data, 'P3911')   as p3911_stw_thesaurus    
    ,get_claims2jsonba(data, 'P2163')   as p2163_fast
    ,get_claims2jsonba(data, 'P1281')   as p1281_woeid    
    ,get_claims2jsonba(data, 'P2347')   as p2347_yso    
    ,get_claims2jsonba(data, 'P1670')   as p1560_lac       
    ,get_claims2jsonba(data, 'P2572')   as p2572_twitter_hastag  

    ,get_claims2jsonba(data, 'P2959')   as p2959_permanent_dupl
    ,get_claims2jsonba(data, 'P18')     as p18_image
    ,get_claims_amount(data, 'P2044')   as p2044_elevation

    ,get_claims2jsonba(data, 'P443')    as p443_pronunciation_audio
    ,get_claims2jsonba(data, 'P898')    as p898_ipa_transcription
    ,get_claims2jsonba(data, 'P281')    as p281_postal_code
    ,get_claims2jsonba(data, 'P856')    as p856_official_website
    ,get_claims2jsonba(data, 'P1581')   as p1581_official_blog    
    ,get_claims2jsonba(data, 'P242')    as p242_locator_map_image
    ,get_claims2jsonba(data, 'P94')     as p94_coat_of_arms_image
    ,get_claims2jsonba(data, 'P41')     as p41_flag_image
    ,get_claims2jsonba(data, 'P935')    as p935_commons_gallery

    ,get_claims2ajsonb_label(data, 'P1151')   as p111_main_wikimedia_portal   
    ,get_claims2jsonba(data, 'P473')    as p473_local_dialing_code

    ,get_monolingualtext_claims2ajsonb(data, 'P1813')   as p1813_short_name
    ,get_monolingualtext_claims2ajsonb(data, 'P1549')   as p1549_demonym
    ,get_monolingualtext_claims2ajsonb(data, 'P1448')   as p1448_official_name
    ,get_monolingualtext_claims2ajsonb(data, 'P1705')   as p1705_native_label
    ,get_monolingualtext_claims2ajsonb(data, 'P1449')   as p1449_nick_name

    ,get_claims_dates(data, 'P580')    as p580_start_time
    ,get_claims_dates(data, 'P582')    as p582_end_time
    ,get_claims_dates(data, 'P571')    as p571_incepion_date
    ,get_claims_dates(data, 'P576')    as p576_dissolved_date
FROM wikidata.wd
ORDER BY data->>'id'::text
;

create index on wikidata.wd_claims ( wd_id );
ANALYSE wikidata.wd_claims ;


\cd :reportdir
\copy (select * from wikidata.wd_claims) TO 'wikidata_wd_claims.csv' CSV  HEADER;

--  Todo:  https://www.wikidata.org/wiki/Q79791 Reconquista (Q79791)  ...
