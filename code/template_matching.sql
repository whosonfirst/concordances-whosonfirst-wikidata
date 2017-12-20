
drop table if exists  :wd_wof_match  CASCADE;
EXPLAIN ANALYZE CREATE UNLOGGED TABLE          :wd_wof_match  as
    select
         wof.* 
        ,ST_Distance( wd.wd_point_merc, wof.wof_geom_merc)::bigint  as _distance
        ,wd.*        
        ,case  when wof.wof_name     = wd.wd_name_en_clean     then 'N1Full-name-match'
               when wof.una_wof_name = wd.una_wd_name_en_clean then 'N3Unaccent-name-match'
               when wof_name_array && wd_name_array            then 'N2Label-name-match'
               when wof_name_array && wd_altname_array         then 'N4Alias-name-match'
               when xxjarowinkler(wof.wof_name_has_num,wd.wd_name_has_num, wof.una_wof_name, wd.una_wd_name_en_clean)>.971   then 'N5JaroWinkler-match'
               when (wd_concordances_array && wof_concordances_array) then 'N6only-Concordances-match'
                                                               else 'Nerr??-checkme-'
         end as  _name_match_type    
    from :wd_input_table   as wd 
        ,:wof_input_table  as wof
    where ( :mcond1
            :mcond2
            :mcond3
          )  
        
    order by wof.id, _distance
;
-- ANALYSE     :wd_wof_match  ;


drop table if exists  :wd_wof_match_agg CASCADE;
CREATE UNLOGGED TABLE  :wd_wof_match_agg  as
with wd_agg as 
(
    select id, wof_name, wof_country, wof_wd_id
        ,  array_agg(wd_id     order by _distance) as a_wd_id
        ,  array_agg(_distance order by _distance) as a_wd_id_distance 
        ,  array_agg(_name_match_type  order by _name_match_type ) as a_wd_name_match_type             
    from :wd_wof_match 
    group by id, wof_name, wof_country, wof_wd_id 
    order by id, wof_name, wof_country, wof_wd_id  
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
        when  array_length(a_wd_id,1) =1   and  array_length(a_wd_id_distance,1)=0  and wof_wd_id  = a_wd_id[1]                    then 'OK-VAL:validated,nodistance;'||a_wd_name_match_type[1]    
        when  array_length(a_wd_id,1) =1   and  array_length(a_wd_id_distance,1)=0  and wof_wd_id != a_wd_id[1] and wof_wd_id !='' then 'OK-REP:suggested for replace,nodistance;'||a_wd_name_match_type[1] 
        when  array_length(a_wd_id,1) =1   and  array_length(a_wd_id_distance,1)=0  and wof_wd_id != a_wd_id[1] and wof_wd_id  ='' then 'OK-ADD:suggested for add,nodistance;'||a_wd_name_match_type[1]

        when  array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and wof_wd_id  = a_wd_id[1]                    then 'OK-VAL:validated-'||a_wd_name_match_type[1]    
        when  array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and wof_wd_id != a_wd_id[1] and wof_wd_id !='' then 'OK-REP:suggested for replace-'||a_wd_name_match_type[1] 
        when  array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and wof_wd_id != a_wd_id[1] and wof_wd_id  ='' then 'OK-ADD:suggested for add-'||a_wd_name_match_type[1]

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
    ,get_wdlabeltext(wd_agg_extended.wof_wd_id)        as old_wd_label
    ,get_wdlabeltext(wd_agg_extended._suggested_wd_id) as new_wd_label
    ,is_cebuano(wd.data)                               as old_is_cebauno
from wd_agg_extended
left join wd.wdx             as wd     on wd_agg_extended.wof_wd_id=wd.wd_id
left join :wd_input_table    as wdnew  on wd_agg_extended._suggested_wd_id=wdnew.wd_id   
;
ANALYSE :wd_wof_match_agg ;



drop table if exists :wd_wof_match_notfound CASCADE;
CREATE UNLOGGED TABLE         :wd_wof_match_notfound  as
with 
disamb as (
    select id, 1 as is_disambiguation
    from wfwd.wof_disambiguation_report
),
redirected as (
    select id, 1 as is_redirected
    from wfwd.wof_wd_redirects
),        
extrdist as (
    select id, distance_km, 1 as is_extreme_distance
    from wfwd.wof_extreme_distance_report
)
,extended_notfound as
(   
    select
        wof.id
        ,wof.wof_name 
        ,wof.wof_country
        ,wof.wof_wd_id
        ,get_wdlabeltext(wof.wof_wd_id)       as old_wd_label
        ,ST_Distance(          
            CDB_TransformToWebmercator( ST_SetSRID(ST_MakePoint(
                 cast(get_wdc_globecoordinate(wd.data,'P625')->0->>'longitude' as double precision)
                ,cast(get_wdc_globecoordinate(wd.data,'P625')->0->>'latitude'  as double precision)
                )
                , 4326))   
            ,wof.wof_geom_merc)::bigint  as _old_distance
        ,wd.a_wof_type
        ,get_wdc_item_label(wd.data,'P31')    as old_p31_instance_of  
        ,get_wdc_item_label(wd.data,'P17')    as old_p17_country_id   
        ,is_cebuano(wd.data)                  as old_is_cebauno   
        ,disamb.is_disambiguation             as old_is_disambiguation
        ,redirected.is_redirected             as old_is_redirected
        ,extrdist.is_extreme_distance         as old_is_extreme_distance
        ,extrdist.distance_km                 as old_ext_distance_km   
/*
        ,case  when wof.wof_name     = wd.wd_name_en_clean     then 'N1Full-name-match'
               when wof.una_wof_name = wd.una_wd_name_en_clean then 'N3Unaccent-name-match'
               when wof_name_array && wd_name_array            then 'N2Label-name-match'
               when wof_name_array && wd_altname_array         then 'N4Alias-name-match'
               when xxjarowinkler(wof.wof_name_has_num,wd.wd_name_has_num, wof.una_wof_name, wd.una_wd_name_en_clean)>.971   then 'N5JaroWinkler-match'
               when (wd_concordances_array && wof_concordances_array) then 'N6only-Concordances-match'
                                                               else 'Nerr??-checkme-'
         end as  _name_match_type   
  */       
    from :wof_input_table as wof
    left join wd.wdx as wd   on wof.wof_wd_id=wd.wd_id
    left join disamb      on disamb.id     = wof.id
    left join extrdist    on extrdist.id   = wof.id   
    left join redirected  on redirected.id = wof.id
    where  wof.id not in ( select id from :wd_wof_match )  
)

select 
    case    when old_is_disambiguation=1                                     then 'Notfound:DEL-Disambiguation'
            when old_is_redirected=1                                         then 'Notfound:DEL-Redirected'           
            when old_is_extreme_distance=1   and  old_ext_distance_km >=1500 then 'Notfound:DEL-Extreme distance 1500-    km'    
            when old_is_extreme_distance=1   and  old_ext_distance_km >=700  then 'Notfound:DEL-Extreme distance  700-1500km'
            when old_is_extreme_distance=1   and  old_ext_distance_km >=400  then 'Notfound:DEL-Extreme distance  400- 700km'
            when old_is_extreme_distance=1   and  old_ext_distance_km >=200  then 'Notfound:DEL-Extreme distance  200- 400km'
            when old_is_extreme_distance=1   and  old_ext_distance_km >=50   then 'Notfound:DEL-Extreme distance   50- 200km'  
            when _old_distance is null and  substr(wof_wd_id,1,1) = 'Q'      then 'Notfound:DEL-Current Wikidataid without coordinate'             
            when _old_distance is not null                                   then 'MAYBE:Notfound-has wikidata, distance is near'              
                                    
                                              else 'Notfound: no wikidaid'
    end as _matching_category  
    ,*
from extended_notfound
order by wof_country, wof_name
;
ANALYSE :wd_wof_match_notfound;




drop table if exists  :wd_wof_match_agg_sum CASCADE;
CREATE UNLOGGED TABLE  :wd_wof_match_agg_sum  as
with 
_matched as (
    select _matching_category, wof_country, wd_number_of_matches, _firstmatch_distance_category, count(*) as N  
    from :wd_wof_match_agg
    group by  _matching_category, wof_country, wd_number_of_matches, _firstmatch_distance_category
    order by  _matching_category, wof_country, wd_number_of_matches, _firstmatch_distance_category 
), 
_notfound as (
    select _matching_category, wof_country, null::int as wd_number_of_matches, null::text as _firstmatch_distance_category, count(*) as N  
    from :wd_wof_match_notfound
    group by  _matching_category, wof_country, wd_number_of_matches, _firstmatch_distance_category
    order by  _matching_category, wof_country, wd_number_of_matches, _firstmatch_distance_category
)

           select * from _matched
union all  select * from _notfound
;

ANALYSE :wd_wof_match_agg_sum ;


\set _pct _pct
drop table if exists  :wd_wof_match_agg_sum:_pct CASCADE;
CREATE UNLOGGED TABLE          :wd_wof_match_agg_sum:_pct  as
with 
 total AS (  select 
            '-- total --' as _matching_category 
           , sum(N) as N
           , null::numeric(10,5) as pct 
           from :wd_wof_match_agg_sum ) 
,msum  as (
    SELECT _matching_category
        , sum(N)       as N
    FROM :wd_wof_match_agg_sum
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
ANALYSE :wd_wof_match_agg_sum:_pct;

select * from :wd_wof_match_agg_sum:_pct;




\set _country _country
drop table if exists  :wd_wof_match_agg_sum:_country CASCADE;
CREATE UNLOGGED TABLE          :wd_wof_match_agg_sum:_country  as
with 
 total as (  
        select 
        '-- total --' as wof_country 
        ,sum (  case when split_part(_matching_category,':',1)= 'Notfound' then N else 0 end ) as n_notfound
        ,sum (  case when split_part(_matching_category,':',1)= 'OK-VAL'   then N else 0 end ) as n_OK_VAL
        ,sum (  case when split_part(_matching_category,':',1)= 'OK-REP'   then N else 0 end ) as n_OK_REP
        ,sum (  case when split_part(_matching_category,':',1)= 'OK-ADD'   then N else 0 end ) as n_OK_ADD
        ,sum (  case when split_part(_matching_category,':',1)= 'WARN'     then N else 0 end ) as n_WARN    
        ,sum (  case when split_part(_matching_category,':',1)= 'ER!R'     then N else 0 end ) as n_ERR_chekme           
        ,sum(N) as _all_        
        from :wd_wof_match_agg_sum
) 
,msum  as (
        select
        wof_country
        ,sum (  case when split_part(_matching_category,':',1)= 'Notfound' then N else 0 end ) as n_notfound
        ,sum (  case when split_part(_matching_category,':',1)= 'OK-VAL'   then N else 0 end ) as n_OK_VAL
        ,sum (  case when split_part(_matching_category,':',1)= 'OK-REP'   then N else 0 end ) as n_OK_REP
        ,sum (  case when split_part(_matching_category,':',1)= 'OK-ADD'   then N else 0 end ) as n_OK_ADD
        ,sum (  case when split_part(_matching_category,':',1)= 'WARN'     then N else 0 end ) as n_WARN 
        ,sum (  case when split_part(_matching_category,':',1)= 'ER!R'     then N else 0 end ) as n_ERR_chekme                   
        ,sum(N) as _all_  
        from :wd_wof_match_agg_sum
        group by  wof_country
        order by n_notfound desc
)
           select * from msum
union all  select * from total
;
ANALYSE :wd_wof_match_agg_sum:_country;
select * from :wd_wof_match_agg_sum:_country;




--select old_p31_instance_of, count(*) as N
--from wd_mlocality_wof_match_notfound
--group by old_p31_instance_of
--order by N desc;
