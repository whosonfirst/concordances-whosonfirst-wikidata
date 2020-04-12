
SET work_mem = '3GB';
SET jit = on;
SET jit_above_cost = 10;


-- time psql -e -f    /wof/code/00_airport.sql

-- select base_name , comp_name_en, base_id,comp_id,  _jarowinkler  from newd.base_wd_match_airports_match where _step='2' and  0.9 > _jarowinkler and  _jarowinkler > 0.85  ;

-- wget http://ourairports.com/airports.csv
-- https://www.iata.org/publications/Pages/code-search.aspx

-- | Bezmer Airport | Bezmer Air Base | 102552663 | Q4900372 | 0.88 |



--
--
-- -- select * from newd.base_wd_match_airports_match_agg_sum_pct;
-- +--------------------------------------------------------+-------+----------+
-- |                   _matching_category                   |   n   |   pct    |
-- +--------------------------------------------------------+-------+----------+
-- | MAYBE0-ADD:Check-Multiple-score,distance,name          |   134 |  0.83380 |
-- | MAYBE0-REP:Check-Multiple-score,distance,name          |    56 |  0.34845 |
-- | MAYBE0-VAL:Check-Multiple-score,distance,name          |    86 |  0.53513 |
-- | MAYBE1-ADD:Extreme distance match (> :safedistance m)  |    27 |  0.16800 |
-- | MAYBE1-REP:Extreme distance match (> :safedistance m)  |     6 |  0.03733 |
-- | MAYBE1-VAL:Extreme distance match (> :safedistance m)  |    31 |  0.19289 |
-- | Notfound1:has name - please debug                      |  4881 | 30.37148 |
-- | Notfound2:DEL-Extreme distance 1500-    km             |     1 |  0.00622 |
-- | OK-ADD:suggested for add-N1Full-name-match             |  2988 | 18.59250 |
-- | OK-ADD:suggested for add-N2Label-name-match            |   334 |  2.07828 |
-- | OK-ADD:suggested for add-N3Unaccent-name-match         |  1397 |  8.69268 |
-- | OK-ADD:suggested for add-N4Alias-name-match            |    52 |  0.32356 |
-- | OK-ADD:suggested for add-N5JaroWinkler-match           |    96 |  0.59735 |
-- | OK-ADD:suggested for add-N6only-Concordances-match     |    15 |  0.09334 |
-- | OK-MultipleMathch-ADD                                  |    32 |  0.19912 |
-- | OK-MultipleMathch-REP                                  |    51 |  0.31734 |
-- | OK-MultipleMathch-VAL                                  |   129 |  0.80269 |
-- | OK-REP:suggested for replace-N1Full-name-match         |    33 |  0.20534 |
-- | OK-REP:suggested for replace-N2Label-name-match        |    13 |  0.08089 |
-- | OK-REP:suggested for replace-N3Unaccent-name-match     |    14 |  0.08711 |
-- | OK-REP:suggested for replace-N4Alias-name-match        |     7 |  0.04356 |
-- | OK-REP:suggested for replace-N6only-Concordances-match |     1 |  0.00622 |
-- | OK-VAL:validated-N1Full-name-match                     |  1568 |  9.75670 |
-- | OK-VAL:validated-N2Label-name-match                    |  1383 |  8.60556 |
-- | OK-VAL:validated-N3Unaccent-name-match                 |   738 |  4.59212 |
-- | OK-VAL:validated-N4Alias-name-match                    |    13 |  0.08089 |
-- | OK-VAL:validated-N5JaroWinkler-match                   |     1 |  0.00622 |
-- | OK-VAL:validated-N6only-Concordances-match             |     1 |  0.00622 |
-- | SUGGESTION10-ADD:Super-score,   check distance,name    |  1877 | 11.67942 |
-- | SUGGESTION10-REP:Super-score,   check distance,name    |     8 |  0.04978 |
-- | SUGGESTION11-ADD:High-score,    check distance,name    |    47 |  0.29245 |
-- | SUGGESTION11-REP:High-score,    check distance,name    |     2 |  0.01244 |
-- | SUGGESTION11-VAL:High-score,    check distance,name    |     1 |  0.00622 |
-- | SUGGESTION12-ADD:Medium-score,  check distance,name    |     9 |  0.05600 |
-- | XMAYBE:Notfound-has wikidata, distance is near         |     2 |  0.01244 |
-- | XMAYBE:Notfound:Current Wikidataid without coordinate  |    31 |  0.19289 |
-- | XMAYBE:Notfound:Extreme distance 50- 200km             |     6 |  0.03733 |
-- | -- total --                                            | 16071 |          |
-- +--------------------------------------------------------+-------+----------+
-- (38 rows)

--
-- Time: 0.504 ms
--
-- real	60m31.993s
-- user	0m0.050s
-- sys	0m0.040s




CREATE OR REPLACE FUNCTION  airports_clean(airport_name text)
    RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE   AS
$func$
select trim( translate( translate( regexp_replace(  nameclean( airport_name ) ,
 $$[[:<:]](de|da|di|domestic|regional|municipal|air base|airport|airpark|aeroport|aeroporto|aeropuerto|lentokentta|letiste|sportflugplatz|sportflughafen|sportflugpl|flugfeld|lufthavn|flughafen|flugplatz|segelflugplatz|segelfluggelande|repuloter|internacional|internationale|luchthaven|flygplats|flugsportverein|aerodrome|airfield|international)[[:>:]]$$,
  ' ',
  'gi'
),'  ',' ') ,'  ',' ')
);
$func$
;


drop table if exists                    newd.wd_match_airports CASCADE;
EXPLAIN ANALYSE 
CREATE /*UNLOGGED*/ TABLE   newd.wd_match_airports  as
select
     wd_id                                                           as comp_id
    ,wd_label                                                        as comp_name_en
    ,check_number(wd_label)                                          as comp_name_has_num
    ,          (regexp_split_to_array( wd_label , '[,()]'))[1]       as comp_name_en_clean
    ,airports_clean((regexp_split_to_array( wd_label , '[,()]'))[1]) as comp_una_name

    ,get_wd_name_array(data)                                         as comp_name_array
    ,get_wd_altname_array(data)                                      as comp_altname_array
    ,get_wd_concordances(data)                                       as comp_concordances_array

    ,cartodb.CDB_TransformToWebmercator(geom::geometry)              as comp_geom_merc
    ,ST_PointOnSurface(geom)                                         as comp_point

   -- extra
    --,(get_wdc_value(data, 'P238'))->>0 as wd_iata
    --,(get_wdc_value(data, 'P239'))->>0 as wd_icao
    --,get_wdc_item(data,'P31')    as p31_instance_of
    --,get_wdc_item_label(data,'P17')    as p17_country_id
    ,nSitelinks
    -- ,get_wdc_value(data, 'P1566')      as p1566_geonames
from wd.wdx
where   (a_wof_type  && ARRAY['campus','P238','P239','P240'])
    and (a_wof_type  @> ARRAY['hasP625'] )
    --and not iscebuano
    --limit 1000
 ---and  wd_label like 'S%'
 --   and geom &&  ST_MakeEnvelope(-112.280,15.547,-92.549,25.721 ,4326)
;

CREATE        INDEX  ON  newd.wd_match_airports USING GIST(comp_geom_merc);
CREATE UNIQUE INDEX  ON  newd.wd_match_airports (comp_id);
ANALYSE newd.wd_match_airports ;





--
---------------------------------------------------------------------------------------
--
\set base_extrafields   ,abbrev,location,iata_code,wikipedia,natlscale

drop table if exists          newd.ne_match_airports CASCADE;
CREATE /*UNLOGGED*/ TABLE         newd.ne_match_airports  as
select
     ogc_fid                                        as base_id
    ,name                                           as base_name
    ,airports_clean(name)                           as base_una_name
    ,check_number(name)                             as base_name_has_num
    ,ARRAY[name::text
          ,airports_clean(name)::text
          ,airports_clean( replace( replace(wikipedia,'http://en.wikipedia.org/wiki/',''),'_',' '))::text
          ,unaccent(name)::text
          ,unaccent(replace( replace(wikipedia,'http://en.wikipedia.org/wiki/',''),'_',' '))::text]
                                                    as base_name_array
    ,ARRAY[
          -- sometimes iata_code = ''
          'iata:code'||iata_code
         ]::text[]                                  as base_concordances_array
    ,cartodb.CDB_TransformToWebmercator(geometry)   as base_geom_merc
    ,ST_PointOnSurface(geometry)                    as base_point
   -- extra
    ,featurecla
    ,wikidataid           as old_comp_id
    ,0::double precision  as min_zoom
    :base_extrafields
from ne.ne_10m_airports
    -- limit 100
--where  name like 'S%'
--where geometry &&  ST_MakeEnvelope(-112.280,15.547,-92.549,25.721 ,4326)
;
CREATE INDEX  ON newd.ne_match_airports  USING GIST(base_geom_merc);
ANALYSE          newd.ne_match_airports;



\set base_extrafields  ,wof_country,base_geom_type,wof_iata_code,wof_icao_code,wof_faa_code,wof_oa_code,wof_wk_page,geom_wof_country,geom_iso_country

drop table if exists   newd.wof_match_campus CASCADE;
CREATE /*UNLOGGED*/ TABLE  newd.wof_match_campus  as
select
     wof.id                                 as base_id
    ,'0'                                    as min_zoom
    ,'airport'                              as featurecla
    ,wof.properties->>'wof:name'            as base_name
    ,check_number(wof.properties->>'wof:name')   as base_name_has_num
    ,airports_clean(wof.properties->>'wof:name') as base_una_name
    ,wof.properties->>'wof:country'         as wof_country
    ,wof.wd_id                              as old_comp_id
    ,get_wof_name_array(wof.properties)     as base_name_array
    ,get_wof_concordances(wof.properties)   as base_concordances_array

    ,wof.properties->'wof:concordances'->> 'iata:code' as wof_iata_code
    ,wof.properties->'wof:concordances'->> 'icao:code' as wof_icao_code
    ,wof.properties->'wof:concordances'->> 'faa:code'  as wof_faa_code
    ,wof.properties->'wof:concordances'->> 'oa:code'   as wof_oa_code
    ,wof.properties->'wof:concordances'->> 'wk:page'   as wof_wk_page
    ,geom.wof_country  as geom_wof_country
    ,geom.iso_country  as geom_iso_country
    , case
     when St_astext( COALESCE( wof.geom::geometry, wof.centroid::geometry ) ) = 'POINT(0 0)'
       then CDB_TransformToWebmercator(  geom.geom::geometry )
       else CDB_TransformToWebmercator(COALESCE( wof.geom::geometry, wof.centroid::geometry  ))
     end                                    as base_geom_merc

    , case
     when St_astext( COALESCE( wof.geom::geometry, wof.centroid::geometry ) ) = 'POINT(0 0)'
       then ST_PointOnSurface(geom.geom::geometry)
       else ST_PointOnSurface(COALESCE( wof.geom::geometry, wof.centroid::geometry  ))
     end                                    as base_point

    , case
     when St_astext( COALESCE( wof.geom::geometry, wof.centroid::geometry ) ) = 'POINT(0 0)'
       then 'GeomMissing'
       else 'OK'
     end                                    as base_geom_type

from     wf.wof_campus   as wof
       left join wf.geom as geom on get_wof_smallesthier(wof.properties)::int8 = geom.id
       where  wof.is_superseded=0  and wof.is_deprecated=0
  --       and St_astext( COALESCE( wof.geom::geometry, wof.centroid::geometry ) ) = 'POINT(0 0)'
  --and  wof.properties->>'wof:name' like 'S%'

 --  limit 100
;
CREATE INDEX  ON newd.wof_match_campus  USING GIST(base_geom_merc);

-- word frequency
SELECT word, count(*) as N
FROM (
  SELECT regexp_split_to_table(base_una_name, '\s') as word
  FROM newd.wof_match_campus
) t
GROUP BY word
order by N desc
LIMIT 100
;


-- \set base_input_table            newd.ne_match_airports
\set base_input_table            newd.wof_match_campus

\set wd_input_table              newd.wd_match_airports


\set base_wd_match               newd.base_wd_match_airports_match
\set base_wd_match_agg           newd.base_wd_match_airports_match_agg
\set base_wd_match_agg_sum       newd.base_wd_match_airports_match_agg_sum
\set base_wd_match_notfound      newd.base_wd_match_airports_match_notfound

\set safedistance        10000
\set searchdistance      50003
\set suggestiondistance   1000
\set __min_jarowinkler    0.70

\set mcond1  ( (comp_concordances_array && base_concordances_array) or (base_una_name = comp_una_name )  or (  comp_name_array && base_name_array ) or (  base_name_array && comp_altname_array ) or (jarowinkler( base_una_name, comp_una_name)>.971 ) )
\set mcond2  and (ST_DWithin ( comp_geom_merc, base_geom_merc , :searchdistance ))
\set mcond3




\set _m1 _m1
\set _m2 _m2

drop table if exists          :base_wd_match:_m1  CASCADE;
EXPLAIN ANALYZE 
CREATE /*UNLOGGED*/ TABLE     :base_wd_match:_m1  as
select
    ST_Distance( wd.comp_geom_merc, base.base_geom_merc)::bigint  as _distance
    ,wd.*
    ,base.*
    ,xxjarowinkler(base.base_name_has_num,wd.comp_name_has_num, base.base_una_name, wd.comp_una_name) as _xxjarowinkler
    ,  jarowinkler(base.base_una_name, wd.comp_una_name)                                              as _jarowinkler
    ,case   when base.base_name      = wd.comp_name_en_clean            then 'N1Full-name-match'
            when base.base_una_name  = wd.comp_una_name                 then 'N3Unaccent-name-match'
            when base_name_array && comp_name_array                     then 'N2Label-name-match'
            when base_name_array && comp_altname_array                  then 'N4Alias-name-match'
            when jarowinkler(base.base_una_name, wd.comp_una_name)>.971 then 'N5JaroWinkler-match'
            when (comp_concordances_array && base_concordances_array)   then 'N6only-Concordances-match'
                                                                        else 'Nerr??-checkme-'
        end as  _name_match_type
    ,'1' as _step
from :wd_input_table    as wd
    ,:base_input_table  as base
where ( :mcond1
        :mcond2
        :mcond3
        )
order by base_id
;

drop table if exists                      :base_input_table:_m2  CASCADE;
EXPLAIN ANALYZE 
CREATE /*UNLOGGED*/ TABLE     :base_input_table:_m2  as
select * from :base_input_table
where  base_id not in ( select distinct base_id from :base_wd_match:_m1 order by base_id )
;
CREATE INDEX  ON :base_input_table:_m2  USING GIST(base_geom_merc);


--drop table if exists                      :wd_input_table:_m2  CASCADE;
--EXPLAIN ANALYZE CREATE /*UNLOGGED*/ TABLE     :wd_input_table:_m2  as
--select * from :wd_input_table
--where  comp_id not in ( select distinct comp_id from :base_wd_match:_m1 order by comp_id )
--;
--CREATE INDEX  ON :wd_input_table:_m2  USING GIST(comp_geom_merc);



drop table if exists                      :base_wd_match:_m2  CASCADE;
EXPLAIN ANALYZE 
CREATE /*UNLOGGED*/ TABLE     :base_wd_match:_m2  as
select
    ST_Distance( comp_geom_merc, base_geom_merc)::bigint  as _distance
    ,wd.*
    ,base.*
    ,xxjarowinkler(base_name_has_num,comp_name_has_num, base_una_name, comp_una_name)  as _xxjarowinkler
    ,  jarowinkler(base_una_name, comp_una_name)  as _jarowinkler
    ,case  when base_name  is null or base_name = '' then 'S1_name_missing_but has_a_candidate'
           when base_name  != ''                     then 'S2JaroWinkler-match~'||  to_char( jarowinkler(base_una_name, comp_una_name) ,'99D9')
                                                     else 'SX-checkme'
        end as  _name_match_type
    ,'2' as _step
from :wd_input_table  /*:_m2 */  as wd
    ,:base_input_table:_m2       as base
where ( (ST_DWithin ( comp_geom_merc, base_geom_merc , :suggestiondistance )))
    and jarowinkler(base_una_name, comp_una_name)  >=   :__min_jarowinkler
order by base_id
;


drop table if exists                      :base_wd_match CASCADE;
EXPLAIN ANALYZE 
CREATE /*UNLOGGED*/ TABLE     :base_wd_match  as
select  case
           when   old_comp_id = comp_id                      then 'VAL'
           when  (old_comp_id ='')  or (old_comp_id is null) then 'ADD'
                                                             else 'REP'
        end
        as _wdstatus
        ,case
            when _distance=0 and (_jarowinkler is not null) then (nsitelinks/40) + 150 + (_jarowinkler*100)
            when _distance=0 and (_jarowinkler is null    ) then (nsitelinks/40) + 150 + 40
            when _distance>0 and (_jarowinkler is not null) then (nsitelinks/40) + 100 - (ln(_distance)*10) + (_jarowinkler*100)
            when _distance>0 and (_jarowinkler is null    ) then (nsitelinks/40) + 100 - (ln(_distance)*10) + 40
        end
        as _score
        ,*
 from
  (          select * from :base_wd_match:_m1
   union all select * from :base_wd_match:_m2 )  as m12
 order by base_id, _score, _distance
;
-- ANALYSE     :base_wd_match  ;




drop table if exists  :base_wd_match_agg CASCADE;
CREATE /*UNLOGGED*/ TABLE :base_wd_match_agg  as
with wd_agg as
(
    select base_id,min_zoom,featurecla,base_name, old_comp_id, base_point
        ,  (array_agg( comp_id           order by _score desc))[1:120] as a_comp_id
        ,  (array_agg(_wdstatus          order by _score desc))[1:120] as a_wdstatus
        ,  (array_agg(_score             order by _score desc))[1:120] as a_comp_id_score
        ,  (array_agg(_distance          order by _score desc))[1:120] as a_comp_id_distance
        ,  (array_agg(_jarowinkler       order by _score desc))[1:120] as a_comp_id_jarowinkler
        ,  (array_agg(_name_match_type   order by _score desc))[1:120] as a_comp_name_match_type
        ,  (array_agg( comp_name_en      order by _score desc))[1:120] as a_comp_name_en
        ,  (array_agg(_step              order by _score desc))[1:120] as a_step
    from :base_wd_match
    group by base_id,min_zoom,featurecla, base_name ,old_comp_id,base_point
    order by base_id,min_zoom,featurecla, base_name ,old_comp_id,base_point
)
, wd_agg_extended as
(
 select wd_agg.*
      ,a_comp_id[1]                                   as _suggested_comp_id
      ,a_wdstatus[1]                                  as _suggested_wdstatus
      ,array_length(a_comp_id,1)                      as wd_number_of_matches
      ,distance_class(a_comp_id_distance[1]::bigint)  as _firstmatch_distance_category
     ,case
        when a_step[1]='1' and array_length(a_comp_id,1) =1   and  array_length(a_comp_id_distance,1)=0  and old_comp_id  = a_comp_id[1]                   then 'OK-VAL:validated,nodistance;'||a_comp_name_match_type[1]
        when a_step[1]='1' and array_length(a_comp_id,1) =1   and  array_length(a_comp_id_distance,1)=0  and old_comp_id != a_comp_id[1] and old_comp_id !='' then 'OK-REP:suggested for replace,nodistance;'||a_comp_name_match_type[1]
        when a_step[1]='1' and array_length(a_comp_id,1) =1   and  array_length(a_comp_id_distance,1)=0  and old_comp_id != a_comp_id[1] and old_comp_id  ='' then 'OK-ADD:suggested for add,nodistance;'||a_comp_name_match_type[1]

        when a_step[1]='1' and array_length(a_comp_id,1) =1   and  a_comp_id_distance[1] <= :safedistance and old_comp_id  = a_comp_id[1]                   then 'OK-VAL:validated-'||a_comp_name_match_type[1]
        when a_step[1]='1' and array_length(a_comp_id,1) =1   and  a_comp_id_distance[1] <= :safedistance and old_comp_id != a_comp_id[1] and old_comp_id !='' then 'OK-REP:suggested for replace-'||a_comp_name_match_type[1]
        when a_step[1]='1' and array_length(a_comp_id,1) =1   and  a_comp_id_distance[1] <= :safedistance and old_comp_id != a_comp_id[1] and old_comp_id  ='' then 'OK-ADD:suggested for add-'||a_comp_name_match_type[1]

        when a_step[1]='1' and (a_comp_id_distance[1] >  :safedistance)    then 'MAYBE1-'||a_wdstatus[1]||':Extreme distance match (> :safedistance m)'

        when a_step[1]='1' and array_length(a_comp_id,1) >1   and  ((a_comp_id_score[1]-a_comp_id_score[2])/a_comp_id_score[1] ) >0.25                     then 'OK-MultipleMathch-'||a_wdstatus[1]
        when a_step[1]='1' and array_length(a_comp_id,1) >1                                                                                          then 'MAYBE0-'||a_wdstatus[1]||':Check-Multiple-score,distance,name'

        when a_step[1]='2' and base_name is not null and a_comp_id_score[1] > 170  then 'SUGGESTION10-'||a_wdstatus[1]||':Super-score,   check distance,name'
        when a_step[1]='2' and base_name is not null and a_comp_id_score[1] > 120  then 'SUGGESTION11-'||a_wdstatus[1]||':High-score,    check distance,name'
        when a_step[1]='2' and base_name is not null and a_comp_id_score[1] >  70  then 'SUGGESTION12-'||a_wdstatus[1]||':Medium-score,  check distance,name'
        when a_step[1]='2' and base_name is not null                               then 'SUGGESTION13-'||a_wdstatus[1]||':Low-score,     check distance,name'

        when a_step[1]='2' and base_name is null and a_comp_id_score[1] > 170  then 'SUGGESTION20-'||a_wdstatus[1]||':base_name_empty:Super-score,   check distance,name'
        when a_step[1]='2' and base_name is null and a_comp_id_score[1] > 120  then 'SUGGESTION21-'||a_wdstatus[1]||':base_name empty:High-score,    check distance,name'
        when a_step[1]='2' and base_name is null and a_comp_id_score[1] >  70  then 'SUGGESTION22-'||a_wdstatus[1]||':base_name empty:Medium-score,  check distance,name'
        when a_step[1]='2' and base_name is null                               then 'SUGGESTION23-'||a_wdstatus[1]||':base_name empty:Low-score,     check distance,name'

        else '?-'||a_wdstatus[1]||':'||a_comp_name_match_type[1]

      end as _matching_category
  from wd_agg
)

, wd_agg_extended_duplicated_id_suggestion as
(
    select _suggested_comp_id
        , count(*) as Ndups_suggestion
    from wd_agg_extended
    where substr(_matching_category,1,2) in (
         'OK'
        ,'MA'
        ,'XM'
        )
        or (_matching_category like 'SUGGESTION10%')
        --or (_matching_category like 'SUGGESTION11%')
        --or (_matching_category like 'SUGGESTION12%')
    group by 1
    having count(*)>1
)
, wd_agg_extended_duplicated_id as
(
    select _suggested_comp_id
        , count(*) as Ndups
    from wd_agg_extended
    where substr(_matching_category,1,2) in (
         'OK'
        ,'MA'
        ,'XM'
        )
    group by 1
    having count(*)>1
)
select wd_agg_extended.*
    , case when  substr(_matching_category,1,2) in ('OK','MA','XM') then dups.Ndups
           else null
      end as Ndups
    ,suggestion_dups.Ndups_suggestion
    ,wdl.wd_label                                       as  name_wikidata
    ,wdl.wd_lang                                        as  name_wd_lang
    ,clean_wdlabel( wdl.data->'labels'->'ar'->>'value') as  name_ar
    ,clean_wdlabel( wdl.data->'labels'->'bn'->>'value') as  name_bn
    ,clean_wdlabel( wdl.data->'labels'->'de'->>'value') as  name_de
    ,clean_wdlabel( wdl.data->'labels'->'en'->>'value') as  name_en
    ,clean_wdlabel( wdl.data->'labels'->'es'->>'value') as  name_es
    ,clean_wdlabel( wdl.data->'labels'->'fr'->>'value') as  name_fr
    ,clean_wdlabel( wdl.data->'labels'->'el'->>'value') as  name_el
    ,clean_wdlabel( wdl.data->'labels'->'hi'->>'value') as  name_hi
    ,clean_wdlabel( wdl.data->'labels'->'hu'->>'value') as  name_hu
    ,clean_wdlabel( wdl.data->'labels'->'id'->>'value') as  name_id
    ,clean_wdlabel( wdl.data->'labels'->'it'->>'value') as  name_it
    ,clean_wdlabel( wdl.data->'labels'->'ja'->>'value') as  name_ja
    ,clean_wdlabel( wdl.data->'labels'->'ko'->>'value') as  name_ko
    ,clean_wdlabel( wdl.data->'labels'->'nl'->>'value') as  name_nl
    ,clean_wdlabel( wdl.data->'labels'->'pl'->>'value') as  name_pl
    ,clean_wdlabel( wdl.data->'labels'->'pt'->>'value') as  name_pt
    ,clean_wdlabel( wdl.data->'labels'->'ru'->>'value') as  name_ru
    ,clean_wdlabel( wdl.data->'labels'->'sv'->>'value') as  name_sv
    ,clean_wdlabel( wdl.data->'labels'->'tr'->>'value') as  name_tr
    ,clean_wdlabel( wdl.data->'labels'->'vi'->>'value') as  name_vi
    ,clean_wdlabel( wdl.data->'labels'->'zh'->>'value') as  name_zh

    ,(get_wdc_value(wdl.data, 'P238'))->>0     as wd_p238_iata
    ,(get_wdc_value(wdl.data, 'P239'))->>0     as wd_p239_icao
    ,(get_wdc_value(wdl.data, 'P240'))->>0     as wd_p240_faa
    ,get_wdc_item_label(wdl.data, 'P931')      as wd_p931_place_served
    ,get_wdc_item_label(wdl.data, 'P131')      as wd_p131_located_in
    ,get_wdc_value(wdl.data,      'P1566')     as wd_p1566_geonames
    ,wdl.data->'sitelinks'->'enwiki'->>'title' as wd_enwiki

    ,ST_X(wdl.geom)                   as wd_long
    ,ST_Y(wdl.geom)                   as wd_lat
    ,ST_X(wd_agg_extended.base_point) as base_long
    ,ST_Y(wd_agg_extended.base_point) as base_lat
    ,wdl.a_wof_type
    ,wd.a_wof_type   as old_a_wof_type
    ,get_wdc_item       (wd.data,'P31')                   as old_p31_instance_of
    ,get_wdc_item      (wdl.data,'P31')                   as new_p31_instance_of
    ,get_wdc_item_label (wd.data,'P17')                   as old_p17_country_id
    ,get_wdc_item_label(wdl.data,'P17')                   as new_p17_country_id
    ,get_wdlabeltext(wd_agg_extended.old_comp_id)         as old_wd_label
    ,get_wdlabeltext(wd_agg_extended._suggested_comp_id)  as new_wd_label
    --,is_cebuano(wd.data)                                  as old_is_cebauno
    ,wd.iscebuano                                         as old_is_cebauno
    ,wdl.iscebuano                                        as new_is_cebauno
    :base_extrafields
from wd_agg_extended
left join wd.wdx                        as wd   on wd_agg_extended.old_comp_id=wd.wd_id
left join wd.wdx                        as wdl  on wd_agg_extended._suggested_comp_id=wdl.wd_id
left join :base_input_table             as base on wd_agg_extended.base_id = base.base_id
left join wd_agg_extended_duplicated_id as dups on wd_agg_extended._suggested_comp_id=dups._suggested_comp_id
left join wd_agg_extended_duplicated_id_suggestion as suggestion_dups
                                                on wd_agg_extended._suggested_comp_id=suggestion_dups._suggested_comp_id

--suggestion_dups.Ndups_suggestion

;
ANALYSE :base_wd_match_agg ;



drop table if exists  :base_wd_match_notfound CASCADE;
CREATE /*UNLOGGED*/ TABLE :base_wd_match_notfound as
with
extended_notfound as
(
    select
         base.base_id
        ,base.min_zoom
        ,base.featurecla
        ,base.base_name
        ,base.old_comp_id
        ,base.base_una_name
        ,base.base_name_has_num
        ,base.base_name_array
        ,base.base_geom_merc
        ,get_wdlabeltext(base.old_comp_id) as old_wd_label
        ,wd.a_wof_type                     as old_a_wof_type
        ,ST_Distance(
            CDB_TransformToWebmercator( wd.geom)
            ,base.base_geom_merc)::bigint  as _old_distance
        ,((ST_Distance(
            CDB_TransformToWebmercator( wd.geom)
            ,base.base_geom_merc))/1000)::bigint  as _old_distance_km
        ,wd.a_wof_type
        ,get_wdc_item_label(wd.data,'P31')    as old_p31_instance_of
        ,get_wdc_item_label(wd.data,'P17')    as old_p17_country_id
        --,is_cebuano(wd.data)                  as old_is_cebauno
        ,wd.iscebuano                         as old_is_cebauno
        :base_extrafields
    from :base_input_table as base
    left join wd.wdx as wd   on base.old_comp_id=wd.wd_id
    where  base.base_id not in ( select base_id from :base_wd_match )
)

select
    case
            when _old_distance_km >=1500 then 'Notfound2:DEL-Extreme distance 1500-    km'
            when _old_distance_km >=700  then 'Notfound3:DEL-Extreme distance  700-1500km'
            when _old_distance_km >=400  then 'Notfound4:DEL-Extreme distance  400- 700km'
            when _old_distance_km >=200  then 'Notfound5:DEL-Extreme distance  200- 400km'
            when _old_distance_km >=50   then 'XMAYBE:Notfound:Extreme distance 50- 200km'
            when _old_distance is null and  substr(old_comp_id,1,1) = 'Q'    then 'XMAYBE:Notfound:Current Wikidataid without coordinate'
            when _old_distance is not null                                   then 'XMAYBE:Notfound-has wikidata, distance is near'

            when base_name is null then 'Notfound0:base_name is NULL'
                                   else 'Notfound1:has name - please debug'
    end as _matching_category
    ,*
from extended_notfound
order by  base_name
;
--ANALYSE :base_wd_match_notfound;


drop table if exists  :base_wd_match_agg_sum CASCADE;
CREATE /*UNLOGGED*/ TABLE :base_wd_match_agg_sum as
with
_matched as (
    select _matching_category,featurecla,  wd_number_of_matches, _firstmatch_distance_category, count(*) as N
    from :base_wd_match_agg
    group by  _matching_category,featurecla, wd_number_of_matches, _firstmatch_distance_category
    order by  _matching_category,featurecla, wd_number_of_matches, _firstmatch_distance_category
),
_notfound as (
    select _matching_category,featurecla,  null::int as wd_number_of_matches, null::text as _firstmatch_distance_category, count(*) as N
    from :base_wd_match_notfound
    group by  _matching_category,featurecla,  wd_number_of_matches, _firstmatch_distance_category
    order by  _matching_category,featurecla,  wd_number_of_matches, _firstmatch_distance_category
)
           select * from _matched
union all  select * from _notfound
;

ANALYSE :base_wd_match_agg_sum ;


\set _pct _pct
drop table if exists  :base_wd_match_agg_sum:_pct CASCADE;
CREATE /*UNLOGGED*/ TABLE :base_wd_match_agg_sum:_pct as
with
 total AS (  select
            '-- total --' as _matching_category
           , sum(N) as N
           , null::numeric(10,5) as pct
           from :base_wd_match_agg_sum )
,msum  as (
    SELECT _matching_category
          , sum(N)       as N
    FROM :base_wd_match_agg_sum
    group by _matching_category
    order by _matching_category
)
select msum._matching_category
      ,msum.N
      ,((100.0 * msum.N ) /total.N)::numeric(10,5) as pct
from msum, total
union all
  select    *
  from total
;
ANALYSE :base_wd_match_agg_sum:_pct;



\set _view _view
DROP VIEW IF EXISTS :base_wd_match_agg:_view CASCADE ;
CREATE VIEW :base_wd_match_agg:_view  as
SELECT
 base_id
,min_zoom
,featurecla
,_matching_category
,base_name
,name_wikidata
,base_geom_type
,nDups
,nDups_suggestion
,case when name_wikidata!=base_name
        then '!'
        else ''
    end as _different_name
,name_wd_lang
,_suggested_comp_id
,_suggested_wdstatus
,case when base_name=unaccent(name_wikidata) and name_wikidata!=base_name
        then name_wikidata
        else ''
    end as _update_name
,a_comp_id
,a_wdstatus
,a_comp_id_score
,a_comp_id_distance
,a_comp_id_jarowinkler
,a_comp_name_match_type
,a_comp_name_en
,a_step
,wd_number_of_matches
,_firstmatch_distance_category
,name_ar
,name_bn
,name_de
,name_en
,name_es
,name_fr
,name_el
,name_hi
,name_hu
,name_id
,name_it
,name_ja
,name_ko
,name_nl
,name_pl
,name_pt
,name_ru
,name_sv
,name_tr
,name_vi
,name_zh
,wd_p238_iata
,wd_p239_icao
,wd_p240_faa
,wd_p931_place_served
,wd_p131_located_in
,wd_p1566_geonames
,wd_enwiki
,wd_long
,wd_lat
,base_long
,base_lat
,a_wof_type
,old_a_wof_type
,old_comp_id
,old_p31_instance_of
,new_p31_instance_of
,old_p17_country_id
,new_p17_country_id
,old_wd_label
,new_wd_label
,old_is_cebauno
,new_is_cebauno
--:base_extrafields
,wof_country
,wof_iata_code,wof_icao_code,wof_faa_code,wof_oa_code,wof_wk_page,geom_wof_country,geom_iso_country
FROM :base_wd_match_agg
ORDER BY _suggested_comp_id , base_name, base_id
;


select * from :base_wd_match_agg_sum:_pct;


--VACUUM;
--



DROP VIEW IF EXISTS newd.airport_usa_01 CASCADE ;
CREATE VIEW         newd.airport_usa_01 as
select 
ST_Distance(
    CDB_TransformToWebmercator(ST_SetSRID(ST_MakePoint(oai.longitude_deg,oai.latitude_deg),4326))
    ,CDB_TransformToWebmercator(ST_SetSRID(ST_MakePoint(arp.wd_long,arp.wd_lat),4326))
)::bigint  as _wd_oa_distance
,
*
  --- count(*) as N
from newd.base_wd_match_airports_match_agg_view as arp
left join oa.airports as oai  on oai.icao_code =arp.wd_p239_icao
where
     wd_p239_icao is not null 
 and new_p17_country_id   = '[{"Q30": "United States of America"}]' 
 --and _matching_category = 'OK-ADD:suggested for add-N1Full-name-match'
 --and _matching_category = 'OK-ADD:suggested for add-N2Label-name-match'
 --and _matching_category = 'OK-ADD:suggested for add-N3Unaccent-name-match'
 --and _matching_category = 'OK-ADD:suggested for add-N4Alias-name-match'
 --and _matching_category = 'OK-ADD:suggested for add-N5JaroWinkler-match'
 --and _matching_category = 'OK-ADD:suggested for add-N6only-Concordances-match'
 and _matching_category like 'OK-ADD:%'
 and ndups_suggestion is null
 and nDups is null
 and base_geom_type = 'GeomMissing'
 and a_comp_id_distance ='{0}'
 and      (a_wof_type  @> ARRAY['campus'] )
 and  not (a_wof_type  @> ARRAY['wof'] )
 and  not (a_wof_type  @> ARRAY['locality'] ) 
 --and not new_is_cebauno
 --and coalesce(oai.local_code,'')  = coalesce(arp.wd_p240_faa,'')
 and coalesce(oai.iata_code,'')   = coalesce(arp.wd_p238_iata,'')
 and coalesce(arp.wd_p240_faa,'') =''
 and coalesce(oai.local_code,'')  =''
 and ST_Distance(
     CDB_TransformToWebmercator(ST_SetSRID(ST_MakePoint(oai.longitude_deg,oai.latitude_deg),4326))
    ,CDB_TransformToWebmercator(ST_SetSRID(ST_MakePoint(arp.wd_long,arp.wd_lat),4326))
)::bigint <= 5000
;
























DROP VIEW IF EXISTS newd.airport_notusa_02 CASCADE ;
CREATE VIEW         newd.airport_notusa_02 as
select 
ST_Distance(
    CDB_TransformToWebmercator(ST_SetSRID(ST_MakePoint(oai.longitude_deg,oai.latitude_deg),4326))
    ,CDB_TransformToWebmercator(ST_SetSRID(ST_MakePoint(arp.wd_long,arp.wd_lat),4326))
)::bigint  as _wd_oa_distance
,
*
  --- count(*) as N
from newd.base_wd_match_airports_match_agg_view as arp
left join oa.airports as oai  on oai.icao_code =arp.wd_p239_icao
where
     wd_p239_icao is not null 
 and new_p17_country_id  != '[{"Q30": "United States of America"}]' 
 --and _matching_category = 'OK-ADD:suggested for add-N1Full-name-match'
 --and _matching_category = 'OK-ADD:suggested for add-N2Label-name-match'
 --and _matching_category = 'OK-ADD:suggested for add-N3Unaccent-name-match'
 --and _matching_category = 'OK-ADD:suggested for add-N4Alias-name-match'
 --and _matching_category = 'OK-ADD:suggested for add-N5JaroWinkler-match'
 --and _matching_category = 'OK-ADD:suggested for add-N6only-Concordances-match'
 and _matching_category like 'OK-ADD:%'
 and ndups_suggestion is null
 and nDups is null
 and base_geom_type = 'GeomMissing'
 and a_comp_id_distance ='{0}'
 and      (a_wof_type  @> ARRAY['campus'] )
 and  not (a_wof_type  @> ARRAY['wof'] )
 and  not (a_wof_type  @> ARRAY['locality'] ) 
 --and not new_is_cebauno
 --and coalesce(oai.local_code,'') = coalesce(arp.wd_p240_faa,'')
 and coalesce(oai.iata_code,'')   = coalesce(arp.wd_p238_iata,'')
 and coalesce(arp.wd_p240_faa,'') =''
 and coalesce(oai.local_code,'')  =''
 and ST_Distance(
     CDB_TransformToWebmercator(ST_SetSRID(ST_MakePoint(oai.longitude_deg,oai.latitude_deg),4326))
    ,CDB_TransformToWebmercator(ST_SetSRID(ST_MakePoint(arp.wd_long,arp.wd_lat),4326))
)::bigint <= 5000
;






DROP VIEW IF EXISTS newd.airport_notusa_02_N1 CASCADE ;
CREATE VIEW         newd.airport_notusa_02_N1 
 as select * from   newd.airport_notusa_02
 where   _matching_category = 'OK-ADD:suggested for add-N1Full-name-match';  

DROP VIEW IF EXISTS newd.airport_notusa_02_N2 CASCADE ;
CREATE VIEW         newd.airport_notusa_02_N2 
 as select * from   newd.airport_notusa_02
 where _matching_category = 'OK-ADD:suggested for add-N2Label-name-match'
;  

DROP VIEW IF EXISTS newd.airport_notusa_02_N3 CASCADE ;
CREATE VIEW         newd.airport_notusa_02_N3 
 as select * from   newd.airport_notusa_02
 where _matching_category = 'OK-ADD:suggested for add-N3Unaccent-name-match'
;  

DROP VIEW IF EXISTS newd.airport_notusa_02_N4 CASCADE ;
CREATE VIEW         newd.airport_notusa_02_N4 
 as select * from   newd.airport_notusa_02
 where _matching_category = 'OK-ADD:suggested for add-N4Alias-name-match'
;  

DROP VIEW IF EXISTS newd.airport_notusa_02_N5 CASCADE ;
CREATE VIEW         newd.airport_notusa_02_N5 
 as select * from   newd.airport_notusa_02
 where _matching_category = 'OK-ADD:suggested for add-N5JaroWinkler-match'
;  

DROP VIEW IF EXISTS newd.airport_notusa_02_N6 CASCADE ;
CREATE VIEW         newd.airport_notusa_02_N6 
 as select * from   newd.airport_notusa_02
 where _matching_category = 'OK-ADD:suggested for add-N6only-Concordances-match'
;  








select count(*) as N from newd.airport_notusa_02_N1
;



--select 
--jarowinkler( arp.base_name , oai.name ) as _jw_name
--,* 
--from newd.airport_usa_01 as arp 
--left join oa.airports as oai  on oai.icao_code =arp.wd_p239_icao
--where 
--      oai.local_code = arp.wd_p240_faa
---- and  oai.iata_code  = arp.wd_p238_iata
--;

--select 
--from newd.airport_notusa_02
  --_wd_oa_distance
  --jarowinkler( base_name , name ) as _jw_wof_oa
  --order by _wd_oa_distance desc

----;
select 
    translate(geom_wof_country::text,'"','')  as wof
  , translate(geom_iso_country::text,'"','')  as iso   
  ,'['||base_name||'](https://spelunker.whosonfirst.org/id/'||base_id||')' as wof_name
  ,'['||name||'](http://ourairports.com/airports/'||id||'/)' as oa_name
  ,'['||name_wikidata||'](https://www.wikidata.org/wiki/'||_suggested_comp_id||')' as wikidata_name  
from newd.airport_notusa_02
where name is not null
order by jarowinkler( base_name , name ) 
;




--/wof/code/cmd_export_csv.sh  newd.airport_usa_01

--/wof/code/cmd_export_csv.sh newd.airport_notusa_02
--/wof/code/cmd_export_csv.sh newd.airport_notusa_02_N1
--/wof/code/cmd_export_csv.sh newd.airport_notusa_02_N2
--/wof/code/cmd_export_csv.sh newd.airport_notusa_02_N3
--/wof/code/cmd_export_csv.sh newd.airport_notusa_02_N4
--/wof/code/cmd_export_csv.sh newd.airport_notusa_02_N5
--/wof/code/cmd_export_csv.sh newd.airport_notusa_02_N6



--/wof/code/cmd_export_csv.sh  newd.base_wd_match_airports_match_agg_view

--   cp  /wof/output/newd.airport_usa_01.csv  /wof/code/air/newd.airport_usa_01.csv  
--echo """
--    -- export --
--    \cd :reportdir
--    \copy (SELECT * FROM newd.base_wd_match_airports_match_agg_view ) TO 'newd.base_wd_match_airports_match_agg_view.csv' DELIMITER ',' CSV HEADER ESCAPE '\"';
--""" | psql -e -vreportdir="${outputdir}"

