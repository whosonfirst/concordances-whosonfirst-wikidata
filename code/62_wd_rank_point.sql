

drop table if exists wikidata.wd_rank_point;
create table wikidata.wd_rank_point as
select
     data->>'id'::text                              as wd_id
    ,ST_SetSRID(ST_MakePoint( 
             cast(get_wdc_globecoordinate(data,'P625')->0->>'longitude' as double precision)
            ,cast(get_wdc_globecoordinate(data,'P625')->0->>'latitude'  as double precision)
            )
    , 4326) as wd_point
FROM wikidata.wd
ORDER BY wd_id
;

ANALYZE wikidata.wd_rank_point
