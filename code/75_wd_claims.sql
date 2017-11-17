


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




drop table if exists wikidata.wd_claims CASCADE;
create table wikidata.wd_claims as
select
    data->>'id'::text                 as wd_id
    ,get_claims2ajsonb(data,'P31')    as p31_instance_of
    ,get_claims2ajsonb(data,'P17')    as p17_country_id     
    ,get_claims2text(data, 'P227')    as p227_gnd_id
    ,get_claims2text(data, 'P300')    as p300_iso3166_2
    ,get_claims2text(data, 'P901')    as p901_fips10_4
    ,get_claims2text(data, 'P214')    as p214_viaf
    ,get_claims2text(data, 'P1997')   as p1997_facebook_places
    ,get_claims2text(data, 'P646')    as p646_freebase
    ,get_claims2text(data, 'P3417')   as p3417_quora_topic
    ,get_claims2text(data, 'P1566')   as p1566_geonames
    ,get_claims2text(data, 'P268')    as p268_bnf
    ,get_claims2text(data, 'P244')    as p244_loc_gov
    ,get_claims2text(data, 'P2959')   as p2959_permanent_dupl
    ,get_claims2text(data, 'P18')     as p18_image
    ,get_claims2text(data, 'P2044')   as p2044_elevation
    ,get_claims2ajsonb(data,'P421')   as p421_timezone
    ,get_claims2text(data, 'P443')    as p443_pronunciation_audio
    ,get_claims2text(data, 'P898')    as p898_ipa_transcription
    ,get_claims2text(data, 'P281')    as p281_postal_code
    ,get_claims2text(data, 'P856')    as p856_official_website
    ,get_claims2text(data, 'P242')    as p242_locator_map_image
    ,get_claims2text(data, 'P94')     as p94_coat_of_arms_image
    ,get_claims2text(data, 'P41')     as p41_flag_image
    ,get_claims2text(data, 'P935')    as p935_commons_gallery
    ,get_claims2text(data, 'P473')    as p473_local_dialing_code
    ,get_claims2ajsonb(data, 'P36')   as p36_capital
    ,get_claims2ajsonb(data, 'P1376') as p1376_capital_of
    ,get_monolingualtext_claims2ajsonb(data, 'P1813')   as p1813_short_name
    ,get_monolingualtext_claims2ajsonb(data, 'P1549')   as p1549_demonym
    ,get_monolingualtext_claims2ajsonb(data, 'P1448')   as p1448_official_name
    ,get_claims2text(data, 'P1449')   as p1449_nick_name
    ,get_claims2ajsonb(data,'P37')    as p37_official_language
    ,get_claims2ajsonb(data,'P47')    as p47_shares_border_with
    ,get_claims2ajsonb(data,'P138')   as p138_name_after
    ,get_claims2text(data, 'P1705')   as p1705_native_label
    ,get_claims2text(data, 'P580')    as p580_start_time
    ,get_claims2text(data, 'P582')    as p582_end_time
    ,get_claims2text(data, 'P571')    as p571_incepion_date
    ,get_claims2text(data, 'P576')    as p576_dissolved_date
FROM wikidata.wd
ORDER BY data->>'id'::text
;

create index on wikidata.wd_claims ( wd_id );
ANALYSE wikidata.wd_claims ;


--  Todo:  https://www.wikidata.org/wiki/Q79791 Reconquista (Q79791)  ...
