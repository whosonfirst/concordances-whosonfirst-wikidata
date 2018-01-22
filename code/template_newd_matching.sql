
drop table if exists                      :ne_wd_match  CASCADE;
EXPLAIN ANALYZE CREATE UNLOGGED TABLE     :ne_wd_match  as
with m1 AS
(
    select
         ST_Distance( wd.wd_point_merc, ne.ne_geom_merc)::bigint  as _distance
        ,wd.*
        ,ne.*
        ,xxjarowinkler(ne.ne_name_has_num,wd.wd_name_has_num, ne.ne_una_name, wd.una_wd_name_en_clean)  as _xxjarowinkler
        ,  jarowinkler(ne.ne_una_name, wd.una_wd_name_en_clean)  as _jarowinkler
       -- ,ST_TRANSFORM(ST_PointOnSurface(ne.ne_geom_merc),4326)   as ne_point
        ,case  when ne.ne_name      = wd.wd_name_en_clean      then 'N1Full-name-match'
               when ne.ne_una_name  = wd.una_wd_name_en_clean  then 'N3Unaccent-name-match'
               when ne_name_array && wd_name_array             then 'N2Label-name-match'
               when ne_name_array && wd_altname_array          then 'N4Alias-name-match'
               when jarowinkler(ne.ne_una_name, wd.una_wd_name_en_clean)>.971   then 'N5JaroWinkler-match'
         --    when (wd_concordances_array && ne_concordances_array) then 'N6only-Concordances-match'
                                                               else 'Nerr??-checkme-'
         end as  _name_match_type
        ,'1' as _step
    from :wd_input_table  as wd
        ,:ne_input_table  as ne
    where ( :mcond1
            :mcond2
            :mcond3
          )
)
,m2 AS
(
    select
         ST_Distance( wd.wd_point_merc, ne.ne_geom_merc)::bigint  as _distance
        ,wd.*
        ,ne.*
        ,xxjarowinkler(ne.ne_name_has_num,wd.wd_name_has_num, ne.ne_una_name, wd.una_wd_name_en_clean)  as _xxjarowinkler
        ,  jarowinkler(ne.ne_una_name, wd.una_wd_name_en_clean)  as _jarowinkler
        --,ST_TRANSFORM(ST_PointOnSurface(ne.ne_geom_merc),4326)   as ne_point
        ,case  when ne.ne_name  is null or ne.ne_name = ''  then 'S1_name_missing_but has_a_candidate'
               when ne.ne_name  != ''                       then 'S2JaroWinkler-match~'||  to_char( jarowinkler(ne.ne_una_name, wd.una_wd_name_en_clean) ,'99D9')
                                                            else 'SX-checkme'
         end as  _name_match_type
        ,'2' as _step
    from :wd_input_table  as wd
        ,:ne_input_table  as ne
    where ( (ST_DWithin ( wd.wd_point_merc, ne.ne_geom_merc , :suggestiondistance ))
          )
          and
          ne.ogc_fid not in ( select distinct ogc_fid from m1 order by ogc_fid )
)

select
        case
            when _distance=0 and (_jarowinkler is not null) then (nsitelinks/40) + 150 + (_jarowinkler*100)
            when _distance=0 and (_jarowinkler is null    ) then (nsitelinks/40) + 150 + 40
            when _distance>0 and (_jarowinkler is not null) then (nsitelinks/40) + 100 - (ln(_distance)*10) + (_jarowinkler*100)
            when _distance>0 and (_jarowinkler is null    ) then (nsitelinks/40) + 100 - (ln(_distance)*10) + 40
        end
        as _score
        ,*
 from
  (          select * from m1
   union all select * from m2 )  as m12
 order by ogc_fid, _score, _distance

;
-- ANALYSE     :ne_wd_match  ;




drop table if exists  :ne_wd_match_agg CASCADE;
CREATE UNLOGGED TABLE :ne_wd_match_agg  as
with wd_agg as
(
    select ogc_fid,min_zoom,featurecla,ne_name, ne_wd_id, ne_point
        ,  array_agg( wd_id             order by _score desc) as a_wd_id
        ,  array_agg(_score             order by _score desc) as a_wd_id_score
        ,  array_agg(_distance          order by _score desc) as a_wd_id_distance
        ,  array_agg(_jarowinkler       order by _score desc) as a_wd_id_jarowinkler
        ,  array_agg(_name_match_type   order by _score desc) as a_wd_name_match_type
        ,  array_agg( wd_name_en        order by _score desc) as a_wd_name_en
        ,  array_agg(_step              order by _score desc) as a_step        
    from :ne_wd_match
    group by ogc_fid,min_zoom,featurecla, ne_name ,ne_wd_id,ne_point
    order by ogc_fid,min_zoom,featurecla, ne_name ,ne_wd_id,ne_point
)
, wd_agg_extended as
(
 select wd_agg.*
      ,a_wd_id[1]                                   as _suggested_wd_id
      ,array_length(a_wd_id,1)                      as wd_number_of_matches
      ,distance_class(a_wd_id_distance[1]::bigint)  as _firstmatch_distance_category
     ,case
        when a_step[1]='1' and array_length(a_wd_id,1) =1   and  array_length(a_wd_id_distance,1)=0  and ne_wd_id  = a_wd_id[1]                   then 'OK-VAL:validated,nodistance;'||a_wd_name_match_type[1]
        when a_step[1]='1' and array_length(a_wd_id,1) =1   and  array_length(a_wd_id_distance,1)=0  and ne_wd_id != a_wd_id[1] and ne_wd_id !='' then 'OK-REP:suggested for replace,nodistance;'||a_wd_name_match_type[1]
        when a_step[1]='1' and array_length(a_wd_id,1) =1   and  array_length(a_wd_id_distance,1)=0  and ne_wd_id != a_wd_id[1] and ne_wd_id  ='' then 'OK-ADD:suggested for add,nodistance;'||a_wd_name_match_type[1]

        when a_step[1]='1' and array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and ne_wd_id  = a_wd_id[1]                   then 'OK-VAL:validated-'||a_wd_name_match_type[1]
        when a_step[1]='1' and array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and ne_wd_id != a_wd_id[1] and ne_wd_id !='' then 'OK-REP:suggested for replace-'||a_wd_name_match_type[1]
        when a_step[1]='1' and array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and ne_wd_id != a_wd_id[1] and ne_wd_id  ='' then 'OK-ADD:suggested for add-'||a_wd_name_match_type[1]

        when a_step[1]='1' and a_wd_id_distance[1] >  :safedistance then 'MAYBE1:Extreme distance match (> :safedistance m)'
        
        when a_step[1]='1' and array_length(a_wd_id,1) >1   and  ((a_wd_id_score[1]-a_wd_id_score[2])/a_wd_id_score[1] ) >0.25                     then 'OK-Multiple-match-TOP-good'
        when a_step[1]='1' and array_length(a_wd_id,1) >1                                                                                          then 'MAYBE0-Check-Multiple-score,distance,name'

        when a_step[1]='2' and ne_name is not null and a_wd_id_score[1] > 170  then 'SUGGESTION10:Super-score,   check distance,name'
        when a_step[1]='2' and ne_name is not null and a_wd_id_score[1] > 120  then 'SUGGESTION11:High-score,    check distance,name'
        when a_step[1]='2' and ne_name is not null and a_wd_id_score[1] >  70  then 'SUGGESTION12:Medium-score,  check distance,name'
        when a_step[1]='2' and ne_name is not null                             then 'SUGGESTION13:Low-score,     check distance,name'

        when a_step[1]='2' and ne_name is null and a_wd_id_score[1] > 170  then 'SUGGESTION20:ne_name_empty:Super-score,   check distance,name'
        when a_step[1]='2' and ne_name is null and a_wd_id_score[1] > 120  then 'SUGGESTION21:ne_name empty:High-score,    check distance,name'
        when a_step[1]='2' and ne_name is null and a_wd_id_score[1] >  70  then 'SUGGESTION22:ne_name empty:Medium-score,  check distance,name'
        when a_step[1]='2' and ne_name is null                             then 'SUGGESTION23:ne_name empty:Low-score,     check distance,name'

        else '?'||a_wd_name_match_type[1]

      end as _matching_category
  from wd_agg
)
select wd_agg_extended.*
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
    ,ST_X(wdl.geom) as wd_long
    ,ST_Y(wdl.geom) as wd_lat
    ,ST_X(ne_point) as ne_long
    ,ST_Y(ne_point) as ne_lat
    ,wdl.a_wof_type
    ,get_wdc_item_label (wd.data,'P31')                as old_p31_instance_of
    ,get_wdc_item_label(wdl.data,'P31')                as new_p31_instance_of
    ,get_wdc_item_label (wd.data,'P17')                as old_p17_country_id
    ,get_wdc_item_label(wdl.data,'P17')                as new_p17_country_id
    ,get_wdlabeltext(wd_agg_extended.ne_wd_id)         as old_wd_label
    ,get_wdlabeltext(wd_agg_extended._suggested_wd_id) as new_wd_label
    ,is_cebuano(wd.data)                               as old_is_cebauno
from wd_agg_extended
left join wd.wdx             as wd     on wd_agg_extended.ne_wd_id=wd.wd_id
left join wd.wdx             as wdl    on wd_agg_extended._suggested_wd_id=wdl.wd_id
;
ANALYSE :ne_wd_match_agg ;



drop table if exists :ne_wd_match_notfound CASCADE;
CREATE UNLOGGED TABLE         :ne_wd_match_notfound  as
with
extended_notfound as
(
    select
         ne.ogc_fid
        ,ne.featurecla
        ,ne.ne_name
        ,ne.ne_wd_id
        ,ne.ne_una_name
        ,ne.ne_name_has_num
        ,ne.ne_name_array
        ,ne.ne_geom_merc
        ,get_wdlabeltext(ne.ne_wd_id)       as old_wd_label
        ,ST_Distance(
            CDB_TransformToWebmercator( wd.geom)
            ,ne.ne_geom_merc)::bigint  as _old_distance
        ,((ST_Distance(
            CDB_TransformToWebmercator( wd.geom)
            ,ne.ne_geom_merc))/1000)::bigint  as _old_distance_km
        ,wd.a_wof_type
        ,get_wdc_item_label(wd.data,'P31')    as old_p31_instance_of
        ,get_wdc_item_label(wd.data,'P17')    as old_p17_country_id
        ,is_cebuano(wd.data)                  as old_is_cebauno
    from :ne_input_table as ne
    left join wd.wdx as wd   on ne.ne_wd_id=wd.wd_id
    where  ne.ogc_fid not in ( select ogc_fid from :ne_wd_match )
)

select
    case
            when _old_distance_km >=1500 then 'Notfound2:DEL-Extreme distance 1500-    km'
            when _old_distance_km >=700  then 'Notfound3:DEL-Extreme distance  700-1500km'
            when _old_distance_km >=400  then 'Notfound4:DEL-Extreme distance  400- 700km'
            when _old_distance_km >=200  then 'Notfound5:DEL-Extreme distance  200- 400km'
            when _old_distance_km >=50   then 'XMAYBE:Notfound:Extreme distance 50- 200km'
            when _old_distance is null and  substr(ne_wd_id,1,1) = 'Q'       then 'XMAYBE:Notfound:Current Wikidataid without coordinate'
            when _old_distance is not null                                   then 'XMAYBE:Notfound-has wikidata, distance is near'

            when ne_name is null then 'Notfound0:ne_name is NULL'
                                 else 'Notfound1:has name - please debug'
    end as _matching_category
    ,*
from extended_notfound
order by  ne_name
;
ANALYSE :ne_wd_match_notfound;




drop table if exists  :ne_wd_match_agg_sum CASCADE;
CREATE UNLOGGED TABLE  :ne_wd_match_agg_sum  as
with
_matched as (
    select _matching_category,featurecla,  wd_number_of_matches, _firstmatch_distance_category, count(*) as N
    from :ne_wd_match_agg
    group by  _matching_category,featurecla, wd_number_of_matches, _firstmatch_distance_category
    order by  _matching_category,featurecla, wd_number_of_matches, _firstmatch_distance_category
),
_notfound as (
    select _matching_category,featurecla,  null::int as wd_number_of_matches, null::text as _firstmatch_distance_category, count(*) as N
    from :ne_wd_match_notfound
    group by  _matching_category,featurecla,  wd_number_of_matches, _firstmatch_distance_category
    order by  _matching_category,featurecla,  wd_number_of_matches, _firstmatch_distance_category
)

           select * from _matched
union all  select * from _notfound
;

ANALYSE :ne_wd_match_agg_sum ;


\set _pct _pct
drop table if exists  :ne_wd_match_agg_sum:_pct CASCADE;
CREATE UNLOGGED TABLE          :ne_wd_match_agg_sum:_pct  as
with
 total AS (  select
            '-- total --' as _matching_category
           , sum(N) as N
           , null::numeric(10,5) as pct
           from :ne_wd_match_agg_sum )
,msum  as (
    SELECT _matching_category
        , sum(N)       as N
    FROM :ne_wd_match_agg_sum
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
ANALYSE :ne_wd_match_agg_sum:_pct;

select * from :ne_wd_match_agg_sum:_pct;


