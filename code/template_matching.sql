\set ON_ERROR_STOP 1




drop table if exists  :wd_wof_match  CASCADE;
create table          :wd_wof_match  as
    select
         wof.* 
        ,ST_Distance(
              CDB_TransformToWebmercator( wd.wd_point)   
            , CDB_TransformToWebmercator(wof.wof_geom) 
            )::bigint     as _distance
        , wd.*        
        , case when  wof.wof_name     = wd.wd_name_en_clean 
              then 'full-name-match'
              else 'unaccent-name-match'
          end as  _name_match_type    
    from :wd_input_table   as wd 
        ,:wof_input_table  as wof
    where ( :mcond1
            :mcond2
            :mcond3
          )  
        
    order by wof.id, _distance
;
ANALYSE     :wd_wof_match  ;


drop table if exists  :wd_wof_match_agg CASCADE;
create table  :wd_wof_match_agg  as
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
        when  array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and wof_wd_id  = a_wd_id[1]                    then 'OKVA:validated-'||a_wd_name_match_type[1]    
        when  array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and wof_wd_id != a_wd_id[1] and wof_wd_id !='' then 'OKRE:suggested for replace-'||a_wd_name_match_type[1] 
        when  array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] <= :safedistance and wof_wd_id != a_wd_id[1] and wof_wd_id  ='' then 'OKAD:suggested for add-'||a_wd_name_match_type[1]
        when  array_length(a_wd_id,1) =1   and  a_wd_id_distance[1] >  :safedistance then 'WARN:Extreme distance match (> :safedistance m)' 
        when  array_length(a_wd_id,1) >1                                             then 'WARN:Multiple_match (please not import this!)' 
        else 'ER!R:check-me'
      end as _matching_category
  from wd_agg
)
select wd_agg_extended.* 
      ,get_wdc_item_label(wd.data,'P31')                 as old_p31_instance_of
      ,wdnew.p31_instance_of                             as new_p31_instance_of      
      ,get_wdc_item_label(wd.data,'P17')                 as old_p17_country_id       
      ,wdnew.p17_country_id                              as new_p17_country_id
      ,get_wdlabeltext(wd_agg_extended.wof_wd_id)        as old_wd_label
      ,get_wdlabeltext(wd_agg_extended._suggested_wd_id) as new_wd_label
      ,is_cebuano(wd.data)                               as old_is_cebauno
from wd_agg_extended
left join wikidata.wd        as wd     on wd_agg_extended.wof_wd_id=wd.data->>'id'
left join :wd_input_table    as wdnew  on wd_agg_extended._suggested_wd_id=wdnew.wd_id   
;
ANALYSE :wd_wof_match_agg ;


drop table if exists  :wd_wof_match_agg_sum CASCADE;
create table  :wd_wof_match_agg_sum  as
    select _matching_category,  wd_number_of_matches, _firstmatch_distance_category, count(*) as N  
    from :wd_wof_match_agg
    group by  _matching_category, wd_number_of_matches, _firstmatch_distance_category
    order by  _matching_category, wd_number_of_matches, _firstmatch_distance_category
;
ANALYSE :wd_wof_match_agg_sum ;



drop table if exists :wd_wof_match_notfound CASCADE;
create table         :wd_wof_match_notfound  as
select
     wof.id
    ,wof.wof_name 
    ,wof.wof_country
    ,wof.wof_wd_id
from :wof_input_table as wof
where  wof.id not in ( select id from :wd_wof_match )  
order by wof.wof_country, wof.wof_name
;
ANALYSE :wd_wof_match_notfound;

