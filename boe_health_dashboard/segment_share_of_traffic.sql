create or replace table etsy-data-warehouse-dev.madelinecollins.segment_share_of_traffic as (
with all_visits as (
  select
    _date
    , platform
    --, browser_platform
    , region
    , count(distinct visit_id) as total_visits
    , sum(total_gms) as total_gms
  from etsy-data-warehouse-prod.weblog.visits
  where _date >= "2022-01-01"
group by all
)
 , segmented_visits as (
  select
  _date
  , buyer_segment
  , platform
  --, browser_platform
  , region
  , count(distinct visit_id) as segment_visits
  , sum(total_gms) as segment_gms
from etsy-data-warehouse-prod.rollups.visits_w_segments
where _date >= "2022-01-01"
group by all
 )
 select
  a._date
  , a.buyer_segment
  , a.platform
 --, a.browser_platform
  , a.region
  , a.segment_visits
  , b.total_visits
  , a.segment_gms
  , b.total_gms
  , a.segment_visits/nullif(b.total_visits,0) as share_total_visits
  , a.segment_gms/ nullif(b.total_gms,0) as share_total_gms
from segmented_visits a
inner join all_visits b using (_date, platform, region)
WHERE _date < CAST(current_date() as DATETIME)
group by all
); 


create or replace table etsy-data-warehouse-dev.madelinecollins.segment_share_of_traffic as (
with all_visits as (
  select
    _date
    , platform
    -- , browser_platform
    , region
    , count(distinct visit_id) as total_visits
    , sum(total_gms) as total_gms
  from etsy-data-warehouse-prod.weblog.visits
  where _date >= "2022-01-01"
group by all
)
 , segmented_visits as (
  select
  _date
  , buyer_segment
  , platform
  --, browser_platform
  , region
  , count(distinct visit_id) as segment_visits
  , sum(total_gms) as segment_gms
from etsy-data-warehouse-prod.rollups.visits_w_segments
where _date >= "2022-01-01"
group by all
 )
 , yy_unions as (
 select
 'ty' as era
  , a._date
  , a.buyer_segment
  , a.platform
 --, a.browser_platform
  , a.region
  , a.segment_visits as segment_visits 
  , b.total_visits  as total_visits -- represents traffic on that day, platform, region
  , a.segment_gms  as segment_gms 
  , b.total_gms as total_gms -- represents traffic on that day, platform, region
  , a.segment_visits/nullif(b.total_visits,0) as share_total_visits
  , a.segment_gms/ nullif(b.total_gms,0) as share_total_gms
from segmented_visits a
inner join all_visits b using (_date, platform, region)
group by all
union all 
 select
 'ly' as era
  , CAST(date_add( a._date, interval 52 WEEK) as DATETIME) AS _date
  , a.buyer_segment
  , a.platform
 --, a.browser_platform
  , a.region
  , a.segment_visits as segment_visits 
  , b.total_visits  as total_visits -- represents traffic on that day, platform, region
  , a.segment_gms  as segment_gms 
  , b.total_gms as total_gms -- represents traffic on that day, platform, region
  , a.segment_visits/nullif(b.total_visits,0) as share_total_visits
  , a.segment_gms/ nullif(b.total_gms,0) as share_total_gms
from segmented_visits a
inner join all_visits b using (_date, platform, region)
group by all
 )
 select 
  era
  , _date
  , buyer_segment
  , platform
 --, browser_platform
  , region
  --ty metrics
  , case when era='ty' then segment_visits end as segment_visits_ty
  , case when era='ty' then segment_visits end as total_visits_ty
  , case when era='ty' then segment_visits end as segment_gms_ty
  , case when era='ty' then segment_visits end as total_gms_ty
  , case when era='ty' then segment_visits end as share_total_visits_ty
  , case when era='ty' then segment_visits end as share_total_gms_ty
  --ly metrics
  , case when era='ly' then segment_visits end as segment_visits_ly
  , case when era='ly' then segment_visits end as total_visits_ly
  , case when era='ly' then segment_visits end as segment_gms_ly
  , case when era='ly' then segment_visits end as total_gms_ly
  , case when era='ly' then segment_visits end as share_total_visits_ly
  , case when era='ly' then segment_visits end as share_total_gms_ly
from 
  yy_unions
WHERE 
    _date < CAST(current_date() as DATETIME)
group by all 
); 
