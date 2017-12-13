


drop table if exists wd.wd_rank_point;
create table wd.wd_rank_point as
select
     data->>'id'::text                              as wd_id
    ,ST_SetSRID(ST_MakePoint( 
             cast(get_wdc_globecoordinate(data,'P625'::text)->0->>'longitude' as double precision)
            ,cast(get_wdc_globecoordinate(data,'P625'::text)->0->>'latitude'  as double precision)
            )
    , 4326) as wd_point
FROM wd.wdx
ORDER BY wd_id
;

create unique index on wd.wd_rank_point  ( wd_id );

ANALYZE wd.wd_rank_point
