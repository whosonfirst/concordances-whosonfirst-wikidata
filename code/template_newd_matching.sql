
drop table if exists                      :ne_wd_match  CASCADE;
EXPLAIN ANALYZE CREATE UNLOGGED TABLE     :ne_wd_match  as
    select
         ST_Distance( wd.wd_point_merc, ne.ne_geom_merc)::bigint  as _distance
        ,wd.*  
        ,ne.* 
        ,xxjarowinkler(ne.ne_name_has_num,wd.wd_name_has_num, ne.ne_una_name, wd.una_wd_name_en_clean)  as _xxjarowinkler
        ,  jarowinkler(ne.ne_una_name, wd.una_wd_name_en_clean)  as _jarowinkler
      
        ,case  when ne.ne_name      = wd.wd_name_en_clean      then 'N1Full-name-match'
               when ne.ne_una_name  = wd.una_wd_name_en_clean  then 'N3Unaccent-name-match'
               when ne_name_array && wd_name_array             then 'N2Label-name-match'
               when ne_name_array && wd_altname_array          then 'N4Alias-name-match'
               when jarowinkler(ne.ne_una_name, wd.una_wd_name_en_clean)>.971   then 'N5JaroWinkler-match'
         --    when (wd_concordances_array && ne_concordances_array) then 'N6only-Concordances-match'
                                                               else 'Nerr??-checkme-'
         end as  _name_match_type    
 
    from :wd_input_table  as wd 
        ,:ne_input_table  as ne
    where ( :mcond1
            :mcond2
            :mcond3
          )  
        
    order by ne.ogc_fid, _distance
;
-- ANALYSE     :ne_wd_match  ;




drop table if exists  :ne_wd_match_agg CASCADE;
CREATE UNLOGGED TABLE :ne_wd_match_agg  as
with wd_agg as 
(
    select ogc_fid,featurecla,ne_name, ne_wd_id
        ,  array_agg( wd_id     order by _distance) as a_wd_id
        ,  array_agg(_distance  order by _distance) as a_wd_id_distance 
        ,  array_agg(_name_match_type  order by _name_match_type ) as a_wd_name_match_type             
    from :ne_wd_match 
    group by ogc_fid,featurecla, ne_name ,ne_wd_id
    order by ogc_fid,featurecla, ne_name ,ne_wd_id
)
, wd_agg_extended as
(
 select wd_agg.*
      ,case 
         when  array_length(a_wd_id,1) =1  then   a_wd_id[1]
           else NULL
        end as _suggested_wd_id
      ,array_length(a_wd_id,1)                      as wd_number_of_matches
      ,distance_class(a_wd_id_distance[1]::bigint)  as _firstmatch_distance_category    
     ,case 
        when  array_length(a_wd_id,1) =1   and  array_length(a_wd_id_distance,1)=0  and ne_wd_id  = a_wd_id[1]                   then 'OK-VAL:validated,nodistance;'||a_wd_name_match_type[1]    
        when  array_length(a_wd_id,1) =1   and  array_length(a_wd_id_distance,1)=0  and ne_wd_id != a_wd_id[1] and ne_wd_id !='' then 'OK-REP:suggested for replace,nodistance;'||a_wd_name_match_type[1] 
        when  array_length(a_wd_id,1) =1   and  array_length(a_wd_id_distance,1)=0  and ne_wd_id != a_wd_id[1] and ne_wd_id  ='' then 'OK-ADD:suggested for add,nodistance;'||a_wd_name_match_type[1]

        when  array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and ne_wd_id  = a_wd_id[1]                   then 'OK-VAL:validated-'||a_wd_name_match_type[1]    
        when  array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and ne_wd_id != a_wd_id[1] and ne_wd_id !='' then 'OK-REP:suggested for replace-'||a_wd_name_match_type[1] 
        when  array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and ne_wd_id != a_wd_id[1] and ne_wd_id  ='' then 'OK-ADD:suggested for add-'||a_wd_name_match_type[1]

        when  array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] >  :safedistance then 'WARN:Extreme distance match (> :safedistance m)' 
        when  array_length(a_wd_id,1) >1                                             then 'WARN:Multiple_match (please not import this!)' 
         else 'ER!R:check-me'
      end as _matching_category
  from wd_agg
)
select wd_agg_extended.* 
    ,wd.a_wof_type
    ,get_wdc_item_label(wd.data,'P31')                 as old_p31_instance_of
    ,wdnew.p31_instance_of                             as new_p31_instance_of      
    ,get_wdc_item_label(wd.data,'P17')                 as old_p17_country_id       
    ,wdnew.p17_country_id                              as new_p17_country_id
    ,get_wdlabeltext(wd_agg_extended.ne_wd_id)         as old_wd_label
    ,get_wdlabeltext(wd_agg_extended._suggested_wd_id) as new_wd_label
    ,is_cebuano(wd.data)                               as old_is_cebauno
from wd_agg_extended
left join wd.wdx             as wd     on wd_agg_extended.ne_wd_id=wd.wd_id
left join :wd_input_table    as wdnew  on wd_agg_extended._suggested_wd_id=wdnew.wd_id   
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
            when _old_distance_km >=1500 then 'Notfound:DEL-Extreme distance 1500-    km'    
            when _old_distance_km >=700  then 'Notfound:DEL-Extreme distance  700-1500km'
            when _old_distance_km >=400  then 'Notfound:DEL-Extreme distance  400- 700km'
            when _old_distance_km >=200  then 'Notfound:DEL-Extreme distance  200- 400km'
            when _old_distance_km >=50   then 'MAYBE:Notfound:Extreme distance 50- 200km'  
            when _old_distance is null and  substr(ne_wd_id,1,1) = 'Q'       then 'MAYBE:Notfound:Current Wikidataid without coordinate'             
            when _old_distance is not null                                   then 'MAYBE:Notfound-has wikidata, distance is near' 

            when ne_name is null then 'Notfound:ne_name is NULL'                            
                                 else 'Notfound:has name - plase debug'
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





