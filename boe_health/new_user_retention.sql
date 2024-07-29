create or replace table etsy-data-warehouse-dev.madelinecollins.boe_user_retention as (
with first_visits as (
  select
  v.browser_id,
  v.browser_platform,
  v.region,
  v._date as first_app_visit,
  v.user_id,
  s.buyer_segment,
  v.event_source,
  v.start_datetime,
  case when v.user_id is not null then 1 else 0 end as is_signed_in,
  lead(v._date) over (partition by v.browser_id order by v.start_datetime asc) as next_visit_date
from `etsy-data-warehouse-prod.weblog.visits` v  
left join etsy-data-warehouse-prod.rollups.visits_w_segments s using (user_id, visit_id)
  where v.platform = "boe"
  and v._date is not null 
  and v.event_source in ("ios", "android")
  group by all
qualify row_number() over(partition by v.browser_id order by start_datetime) = 1
)
select 
is_signed_in,
browser_platform,
region,
buyer_segment,--segment when they downloaded the app
-- agg totals
count(distinct case when first_app_visit = next_visit_date then browser_id end) as next_day_visits,
count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end) as first_7_days,
count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end) as first_14_days,
count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end) as first_30_days,
--pct
-- count(distinct case when first_app_visit = next_visit_date then browser_id end)/count(distinct browser_id) as pct_next_day_visits,
-- count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end)/count(distinct browser_id) as pct_first_7_days,
-- count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end)/count(distinct browser_id) as pct_first_14_days,
-- count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end)/count(distinct browser_id) as pct_first_30_days
from first_visits
where first_app_visit <= current_date
group by all
);
