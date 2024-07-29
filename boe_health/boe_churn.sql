--start with all visit data 
with agg_visit as (
select
 a.user_id
  , a.platform 
  , a.browser_platform
	, a.region  
  , a.is_admin_visit as admin
  , a.top_channel 
  , a.visit_id
  , a._date
  , a.start_datetime
  , unix_seconds(timestamp (a.start_datetime)) as start_time
  , row_number() over (partition by a.user_id order by unix_seconds(timestamp (a.start_datetime)) desc, a.visit_id desc) AS visit_order
  , b.buyer_segment
from 
  etsy-data-warehouse-prod.weblog.visits a 
left join 
  etsy-data-warehouse-prod.rollups.visits_w_segments b using (visit_id, user_id, _date, platform)
where _date >= current_date-10
)
, last_visit_platform as (
select 
  user_id
  , region
  , admin
  , platform
  , buyer_segment
  , count(distinct visit_id) as total_visits
  , min(visit_order) as most_recent_visit
  , cast(max(_date) as date) as most_recent_visit_date
from agg_visit
group by all 
)
, agg as (
select
    user_id
  , region
  , admin
  , buyer_segment
  , current_date as _date
  , sum(total_visits) as total_visits
  , coalesce(max(case when platform in ('boe') then date_diff(current_date, most_recent_visit_date, day) else null end),0) as days_since_boe_visit
  , coalesce(max(case when platform in ('mobile_web') then date_diff(current_date, most_recent_visit_date, day) else null end),0) as days_since_mweb_visit
  , coalesce(max(case when platform in ('desktop') then date_diff(current_date, most_recent_visit_date, day)else null end),0) as days_since_desktop_visit
from 
  last_visit_platform
group by all 
)
select
  _date
  , region
  , admin
  , buyer_segment
  , days_since_boe_visit
  , days_since_mweb_visit
  , days_since_desktop_visit
  , count(distinct user_id) as unique_users
from agg
group by all 
