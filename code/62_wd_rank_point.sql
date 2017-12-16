


drop table if exists wd.wd_rank_point;
CREATE UNLOGGED TABLE wd.wd_rank_point as
select
     wd_id
    ,ST_SetSRID(ST_MakePoint( 
             cast(get_wdc_globecoordinate(data,'P625'::text)->0->>'longitude' as double precision)
            ,cast(get_wdc_globecoordinate(data,'P625'::text)->0->>'latitude'  as double precision)
            )
    , 4326) as wd_point
FROM wd.wdx
ORDER BY wd_id
;

create unique index on wd.wd_rank_point  ( wd_id )  WITH (fillfactor = 100);

ANALYZE wd.wd_rank_point
