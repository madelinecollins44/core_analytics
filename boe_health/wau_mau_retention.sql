------code from https://etsy.slack.com/archives/D063LTSJ1GT/p1722348600396999

-- WAU Retention
create or replace table etsy-data-warehouse-dev.rollups.boe_waus_retention as (
with waus as (
  select 
    date_trunc(_date, week) as week,
    v.user_id,
    count(*) as visits,
    sum(total_gms) as gms
  from `etsy-data-warehouse-prod.weblog.visits` v  
  where platform = "boe"
    and _date >= "2019-12-01"
    and canonical_region = "US"
    and _date != "2024-02-29"
    and event_source = "ios"
    and v.user_id > 0
  group by all
  ),
next_visit_weeks as (
  select *,
    lead(week) over (partition by user_id order by week asc) as next_visit_week 
  from waus 
  )
select 
  week,
  count(*) as waus,
  count(case when next_visit_week = date_add(week, interval 1 week) then user_id end) as retained,
  count(case when next_visit_week = date_add(week, interval 1 week) then user_id end) / count(*) as pct_retained,
  sum(gms) as gms
from next_visit_weeks 
group by all
order by 1
); 

-- MAU Retention
create or replace table etsy-data-warehouse-dev.rollups.boe_maus_retention as (
with maus as (
  select 
    date_trunc(_date, month) as month,
    v.user_id,
    count(*) as visits,
    sum(total_gms) as gms
  from `etsy-data-warehouse-prod.weblog.visits` v  
  where platform = "boe"
    and _date >= "2019-12-01"
    and canonical_region = "US"
    and _date != "2024-02-29"
    and event_source = "ios"
    and v.user_id > 0
  group by all
  ),
next_visit_months as (
  select *,
    lead(month) over (partition by user_id order by month asc) as next_visit_month
  from waus 
  )
select 
  month,
  count(*) as maus,
  count(case when next_visit_month = date_add(month, interval 1 month) then user_id end) as retained,
  count(case when next_visit_month = date_add(month, interval 1 month) then user_id end) / count(*) as pct_retained,
  sum(gms) as gms
from next_visit_months 
group by all
order by 1
); 
