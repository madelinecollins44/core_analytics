with first_visits as (
  select
  browser_id,
  browser_platform,
  region,
  _date as first_app_visit,
  user_id,
  event_source,
  start_datetime,
  case when user_id is not null then 1 else 0 end as is_signed_in,
  lead(_date) over (partition by browser_id order by start_datetime asc) as next_visit_date
from `etsy-data-warehouse-prod.weblog.visits` v  
  where platform = "boe"
  and _date is not null 
  and event_source in ("ios", "android")
  group by all
qualify row_number() over(partition by browser_id order by start_datetime) = 1
)
select 
is_signed_in,
count(distinct case when first_app_visit = next_visit_date then browser_id end) as next_day_visits,
count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end) as first_7_days,
count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end) as first_14_days,
count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end) as first_30_days
from `etsy-data-warehouse-dev.semanuele.first_visits`
--where event_source = "ios"
--and 
where first_app_visit <= current_date
group by all
