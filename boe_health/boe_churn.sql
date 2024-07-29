--start with all visit data 
with agg_visit as (
select
  user_id
  , platform 
  , browser_platform
	, region  
  , is_admin_visit as admin
  , top_channel 
  , visit_id
  , _date
  , start_datetime
  , unix_seconds(timestamp (start_datetime)) as start_time
  , row_number() over (partition by user_id order by unix_seconds(timestamp (start_datetime)) desc, visit_id desc) AS visit_order
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-10
)
, last_visit_platform as (
select 
  user_id
  , platform
  , min(visit_order) as most_recent_visit
from agg_visit
where user_id=266926560
group by all 
)
select
  a.user_id
  , a.platform 
  , a.browser_platform
	, a.region  
  , a.admin
  , a.top_channel 
  , case when b.platform in ('boe') then (current_date- a._date) end as days_since_boe_visit
  -- days_since_mweb_visit
  -- days_since_detskop_visit
from 
  agg_visit a 
left join 
  last_visit_platform b
    on a.user_id=b.user_id
    and a.platform=b.platform
    and a.visit_order=b.most_recent_visit
