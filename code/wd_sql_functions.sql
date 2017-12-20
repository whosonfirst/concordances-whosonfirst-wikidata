

-- https://github.com/maxlath/wikidata-sdk/blob/master/docs/install.md
-- on truthyness: https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Truthy_statements
-- https://github.com/nichtich/wikidata-taxonomy
        

drop table if exists  codes.wd2country CASCADE;
CREATE UNLOGGED TABLE          codes.wd2country  as
select   wof.wd_id
        ,wof.id
        ,wof.properties->>'wof:name'                    as wof_name 
        ,wof.properties->>'wof:country'                 as wof_country
from wf.wof_country as wof
where is_superseded=0 and is_deprecated=0
;
-- quick fix :  Update Norway - now: Q11965730  -> should be 'Q20' ;  
update codes.wd2country set wd_id='Q20' WHERE wof_country='NO' ;

---
CREATE UNIQUE INDEX  ON codes.wd2country (wd_id)        WITH (fillfactor = 100);
CREATE UNIQUE INDEX  ON codes.wd2country (wof_country)  WITH (fillfactor = 100);    
ANALYSE codes.wd2country;


CREATE OR REPLACE FUNCTION public.get_countrycode(wdid text)
RETURNS text
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    SELECT wof_country
    FROM codes.wd2country
    WHERE wd_id=wdid
    ;
$$;
-- select get_countrycode('Q30');



CREATE OR REPLACE FUNCTION distance_class(_distance bigint)
RETURNS text
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    SELECT
      case 
          when _distance <=   5000 then '000-005km' 
          when _distance <=  10000 then '005-010km' 
          when _distance <=  15000 then '010-015km'
          when _distance <=  20000 then '015-020km'
          when _distance <=  25000 then '020-025km'
          when _distance <=  30000 then '025-030km'
          when _distance <=  35000 then '030-035km'
          when _distance <=  40000 then '035-040km'
          when _distance <=  45000 then '040-045km'
          when _distance <=  50000 then '045-050km'
          when _distance <=  60000 then '050-060km'    
          when _distance <=  70000 then '060-070km'    
          when _distance <=  80000 then '070-080km'    
          when _distance <=  90000 then '080-090km'
          when _distance <= 100000 then '090-099km'
          when _distance <= 150000 then '100-150km'
          when _distance <= 200000 then '150-200km'    
          when _distance <= 250000 then '200-250km'    
          when _distance <= 300000 then '250-300km'    
          when _distance <= 400000 then '300-400km'    
          when _distance <= 500000 then '400-500km'    
          when _distance <= 600000 then '500-600km'    
          when _distance <= 700000 then '600-700km'    
          when _distance <= 800000 then '700-800km'    
          when _distance <= 900000 then '800-900km'    
          when _distance <=1000000 then '900-999km'                                                                                                              
                                   else '..>1000km'     
      end;
$$;
-- select distance_class(24999);




CREATE OR REPLACE FUNCTION public.is_cebuano(data jsonb)
RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    with cebu_calc as      
    (
        SELECT sum(    
        case when site in ( 'enwiki','dewiki','ptwiki','eswiki','ruwiki','frwiki','nlwiki','rowiki')   then 10
                when site in ( 'svwiki','shwiki' )   then  4
                when site in ( 'cebwiki')            then -9      
                                                     else  5
        end
        ) site_points
        FROM jsonb_object_keys(data->'sitelinks') as site
    )
    select case when site_points > 0 then false
                                     else true
        end 
    from cebu_calc    
    ;
$$;


-- https://www.wikidata.org/wiki/Help:Dates
CREATE OR REPLACE FUNCTION public.get_wdc_date(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    select  jsonb_agg( split_part(timevalue,'T',1)) 
    from (
        SELECT  *,claimorder
        FROM(
            SELECT wdp ->'mainsnak'->'datavalue'->'value'->>'time'  as timevalue
                   ,1 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='preferred'
          UNION ALL
            SELECT wdp ->'mainsnak'->'datavalue'->'value'->>'time'  as timevalue
                  ,2 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='normal'
        ) s     
        ORDER BY claimorder
    ) t 
    ;
$$;


CREATE OR REPLACE FUNCTION public.get_wdlabel(wdid text)
RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    SELECT jsonb_build_object(wd_id, wd_label)          
    FROM wdlabels.en
    WHERE wd_id=wdid
    ;
$$;

CREATE OR REPLACE FUNCTION public.get_wdlabeltext(wdid text)
RETURNS text
IMMUTABLE STRICT PARALLEL SAFE
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

CREATE OR REPLACE FUNCTION public.get_wdc_population(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE
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
            WHERE  wdp->>'rank'='preferred' 
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
            WHERE wdp->>'rank'='normal'  
        ) s
        order BY claimorder     
    ) t
 
    ;
$$;




CREATE OR REPLACE FUNCTION public.get_wdc_item_label(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    select  jsonb_agg(get_wdlabel(dataitem)) 
    from (
        SELECT  *,claimorder
        FROM(
            SELECT wdp ->'mainsnak'->'datavalue'->'value'->>'id'  as dataitem
                   ,1 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='preferred'
          UNION ALL
            SELECT wdp ->'mainsnak'->'datavalue'->'value'->>'id'  as dataitem
                  ,2 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='normal'
        ) s     
        ORDER BY claimorder
    ) t 
    ;
$$;

CREATE OR REPLACE FUNCTION public.get_wdc_item(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    select  jsonb_agg(dataitem) 
    from (
        SELECT  *,claimorder
        FROM(
            SELECT wdp ->'mainsnak'->'datavalue'->'value'->>'id'  as dataitem
                   ,1 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='preferred'
          UNION ALL
            SELECT wdp ->'mainsnak'->'datavalue'->'value'->>'id'  as dataitem
                  ,2 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='normal'
        ) s     
        ORDER BY claimorder
    ) t 
    ;
$$;


CREATE OR REPLACE FUNCTION public.get_wdc_value(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    select  jsonb_agg(datavalue) 
    from (

        SELECT  *,claimorder
        FROM(
            SELECT wdp ->'mainsnak'->'datavalue'->>'value'  as datavalue
                   ,1 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='preferred'
          UNION ALL
            SELECT wdp ->'mainsnak'->'datavalue'->>'value'  as datavalue
                  ,2 as claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE wdp->>'rank'='normal'
        ) s     
        ORDER BY claimorder
    ) t 
    ;
$$;

CREATE OR REPLACE FUNCTION public.get_wdc_monolingualtext(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
   select  jsonb_agg( lang ) 
    from (

        SELECT  *,claimorder
        FROM(
            SELECT jsonb_build_object( 
                     wdp ->'mainsnak'->'datavalue'->'value'->>'language'  
                    ,wdp ->'mainsnak'->'datavalue'->'value'->>'text' 
                    ) as lang
                    ,1 as claimorder
            FROM   jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE  wdp->>'rank'='preferred'
          UNION ALL
            SELECT jsonb_build_object( 
                     wdp ->'mainsnak'->'datavalue'->'value'->>'language'  
                    ,wdp ->'mainsnak'->'datavalue'->'value'->>'text' 
                    ) as lang
                    ,2 as claimorder
            FROM   jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE  wdp->>'rank'='normal'
        ) s
        ORDER BY claimorder
    ) t 
$$;

CREATE OR REPLACE FUNCTION public.get_claims_amount(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    select  jsonb_agg( amountvalue ) 
    from (

        SELECT  *,claimorder
        FROM(
            SELECT wdp ->'mainsnak'->'datavalue'->'value'->>'amount'  as amountvalue
                  ,1 as claimorder
            FROM   jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE  wdp->>'rank'='preferred'
          UNION ALL
            SELECT wdp ->'mainsnak'->'datavalue'->'value'->>'amount'  as amountvalue
                   ,2 as claimorder
            FROM   jsonb_array_elements( data->'claims'->wdproperty ) as wdp
            WHERE  wdp->>'rank'='normal'
        ) s
        ORDER BY claimorder
    ) t       
    ;
$$;


-- [{"latitude": "-40.7", "longitude": "-66.15"}]
-- [{"latitude": "-32.6", "longitude": "-66.125"}]
-- [{"latitude": "-28.05", "longitude": "-58.233333333333"}, {"latitude": "-28.04532", "longitude": "-58.22835"}]

CREATE OR REPLACE FUNCTION public.get_wdc_globecoordinate(data jsonb, wdproperty text)
RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    SELECT  jsonb_agg(coord) 
    FROM (
        SELECT  *,claimorder
        FROM(
            SELECT
            	jsonb_build_object( 'latitude' ,wdp ->'mainsnak'->'datavalue'->'value'->>'latitude'
                                   ,'longitude',wdp ->'mainsnak'->'datavalue'->'value'->>'longitude'
                ) AS coord
                ,1 AS claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) AS wdp
            WHERE  wdp->>'rank'='preferred'  and  wdp->'mainsnak'->'datavalue'->>'type' = 'globecoordinate'
          UNION ALL
            SELECT
            	jsonb_build_object( 'latitude' ,wdp ->'mainsnak'->'datavalue'->'value'->>'latitude'
                                   ,'longitude',wdp ->'mainsnak'->'datavalue'->'value'->>'longitude'
                ) AS coord
                ,2 AS claimorder
            FROM jsonb_array_elements( data->'claims'->wdproperty ) AS wdp
            WHERE wdp->>'rank'='normal'  and  wdp->'mainsnak'->'datavalue'->>'type' = 'globecoordinate'
        ) s
        ORDER BY claimorder     
    ) t
    ;
$$;


















CREATE OR REPLACE FUNCTION get_wof_name_array(properties JSONB)
RETURNS text[]
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
	with xwof_rec as
	(
		select 
		    jsonb_object_keys(properties)              as wof_property
		   ,properties->jsonb_object_keys(properties)  as wof_jvalue
		   ,jsonb_typeof( properties->jsonb_object_keys(properties) ) as wof_jtype 
	)
	select  
	    array_agg( DISTINCT  wof_value order by   wof_value)
	from xwof_rec 
	    ,jsonb_array_elements_text(wof_jvalue) with ordinality as a(wof_value,wof_arrayorder)
	where wof_jtype='array' and  ( wof_property ~ '^name:.*_x_(preferred|variant|colloquial|historical)' )
                            and  ( wof_property not like 'name:ceb_%')
	   ;
$$;
--select ( get_wof_name_array(properties)) from wf.wof_country limit 4;



CREATE OR REPLACE FUNCTION get_wd_name_array(data JSONB)
RETURNS text[]
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
   select   array_agg( distinct value->>'value' order by value->>'value' )
   FROM jsonb_each(data->'labels') as l 
   WHERE l.value->>'language' != 'ceb'
   ;
$$;
-- select get_wd_name_array(data)  from wd.wdx limit 10;


CREATE OR REPLACE FUNCTION get_wd_altname_array(data JSONB)
RETURNS text[]
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
   select   array_agg( distinct value->>'value' order by value->>'value' )
   FROM jsonb_each(data->'aliases') as l 
   WHERE l.value->>'language' != 'ceb'
   ;
$$;



CREATE OR REPLACE FUNCTION jsonb_arr2distinct_textarr(jsonb_arr jsonb,prefix text)
   RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE  AS
$$
    SELECT 
    ARRAY(SELECT DISTINCT prefix||jsonb_array_elements_text(jsonb_arr)  ORDER BY 1 )
$$
;

CREATE OR REPLACE FUNCTION get_wd_concordances(data JSONB)
   RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE  AS
$$

    select   array_cat( array_cat(array_cat( array_cat(  
          jsonb_arr2distinct_textarr(get_wdc_value(data, 'P1566'),'gn:id:')
         ,jsonb_arr2distinct_textarr(get_wdc_value(data, 'P882'),'fips:code:')
    )
         ,jsonb_arr2distinct_textarr(get_wdc_value(data, 'P901'),'fips:code:') 
    )
         ,jsonb_arr2distinct_textarr(get_wdc_value(data, 'P238'),'iata:') 
    )
         ,jsonb_arr2distinct_textarr(get_wdc_value(data, 'P239'),'icao:') 
    )        
$$
;



CREATE OR REPLACE FUNCTION get_wof_concordances_element(properties JSONB,concordances_id TEXT, prefix TEXT )
   RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE  AS
$$

        select 
         case when properties->'wof:concordances'->> concordances_id is not null   
              then prefix||  (properties->'wof:concordances'->> concordances_id ) 
              else null 
         end
$$
;


CREATE OR REPLACE FUNCTION get_wof_concordances(properties JSONB)
   RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
$$
    select 
    array_remove(
    ARRAY[
          get_wof_concordances_element(properties,'fips:code','fips:code:' )
        , get_wof_concordances_element(properties,'gn:id'    ,'gn:id:'     )
        , get_wof_concordances_element(properties,'iata'     ,'iata:'      )
        , get_wof_concordances_element(properties,'icao'     ,'icao:'      )
        ]
    ,
    null)
$$   
;



CREATE OR REPLACE FUNCTION  nameclean(vname text) 
    RETURNS text  
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE   AS
$$
select 
    replace( 
        replace( 
            translate( 
                lower( unaccent ( vname ) ) 
                ,'-_'
                ,'  ') 
        ,'  ',' ')  
    ,'  ',' ')
$$
;
-- select nameclean('Al-City') =  nameclean('Al - City') ;



CREATE OR REPLACE FUNCTION check_number(mytext text)
-- check roman numbers to (1-32)  or any arab number  
RETURNS BOOLEAN
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    SELECT ( mytext ~ '[0-9]' )
         or (mytext ~ '\y(I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII|XIII|XIV|XV|XVI|XVII|XVIII|XIX|XX|XXI|XXII|XXIII|XXIV|XXV|XXVI|XXVII|XXVIII|XXIX|XXX|XXXI|XXXII)\y' )
;
$$;


CREATE OR REPLACE FUNCTION xxjarowinkler( text1_has_num BOOLEAN,text2_has_num BOOLEAN, simpl_text1 text, simpl_text2 text)
-- only check jarowinkler if not contains any number ...
RETURNS float8
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE sql
AS $$
    SELECT
    case  when (not text1_has_num) and (not text2_has_num ) 
                then jarowinkler(simpl_text1,simpl_text2)
                else 0
    end
$$;