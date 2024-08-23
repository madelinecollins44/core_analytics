------code from https://etsy.slack.com/archives/D063LTSJ1GT/p1722348600396999

-- WAU Retention
create or replace table etsy-data-warehouse-dev.rollups.boe_waus_retention as (
with waus as (
  select 
    date_trunc(_date, week) as week,
    -- buyer_segment,
    top_channel,
    browser_platform,
    region,
    case when v.user_id is not null then 1 else 0 end as signed_in,
    b.mapped_user_id,
    count(distinct v.visit_id) as visits,
    sum(v.total_gms) as gms,
  from 
    etsy-data-warehouse-prod.weblog.visits v 
  left join  
    etsy-data-warehouse-prod.user_mart.user_mapping b 
      on v.user_id=b.user_id 
  -- left join 
  --   buyer_segment s 
  --     on b.mapped_user_id=s.mapped_user_id
  --     and v_date= s.as_of_date
  where platform = "boe"
    and _date >= "2024-06-01"
    and _date != "2024-02-29"
  group by all
  )
-- next, grab the visit info that appears the most for each mapped_user_id. since its tracking user_ids, we cannot track signed out 
, common_visit_info as (
select
  --buyer_segment,
  top_channel,
  browser_platform,
  region,
  signed_in,
  mapped_user_id,
  visits,
from waus
qualify row_number() over(partition by mapped_user_id order by visits) = 1
)
--now get the visits from the following week for each mapped user id
, next_visit_week as (
select  
  week, 
  visits, 
  gms,
  mapped_user_id, 
  lead(week) over (partition by mapped_user_id order by week asc) as next_visit_week 
from waus 
)
  select
  nw.week,
  -- vi.buyer_segment, 
  vi.top_channel,
  vi.browser_platform,
  vi.region,
  vi.signed_in,
  count(nw.mapped_user_id) as waus, 
  count(case when nw.next_visit_week = date_add(week, interval 1 week) then mapped_user_id end) as retained,
  count(case when nw.next_visit_week = date_add(week, interval 1 week) then mapped_user_id end) / count(*) as pct_retained,
  sum(gms) as gms
from 
  next_visit_week nw 
left join 
  common_visit_info vi
    using (mapped_user_id)
group by all 
); 

-- MAU Retention
create or replace table etsy-data-warehouse-dev.rollups.boe_maus_retention as (
with maus as (
  select 
    date_trunc(_date, month) as month,
    -- buyer_segment,
    top_channel,
    browser_platform,
    region,
    case when v.user_id is not null then 1 else 0 end as signed_in,
    b.mapped_user_id,
    count(distinct v.visit_id) as visits,
    sum(v.total_gms) as gms,
  from 
    etsy-data-warehouse-prod.weblog.visits v 
  left join  
    etsy-data-warehouse-prod.user_mart.user_mapping b 
      on v.user_id=b.user_id 
  -- left join 
  --   buyer_segment s 
  --     on b.mapped_user_id=s.mapped_user_id
  --     and v_date= s.as_of_date
  where platform = "boe"
    and _date >= "2024-06-01"
    and _date != "2024-02-29"
  group by all
  )
-- next, grab the visit info that appears the most for each mapped_user_id. since its tracking user_ids, we cannot track signed out 
, common_visit_info as (
select
  --buyer_segment,
  top_channel,
  browser_platform,
  region,
  signed_in,
  mapped_user_id,
  visits,
from maus
qualify row_number() over(partition by mapped_user_id order by visits) = 1
)
--now get the visits from the following month for each mapped user id
, next_visit_month as (
select  
  month, 
  visits, 
  gms,
  mapped_user_id, 
  lead(month) over (partition by mapped_user_id order by month asc) as next_visit_month 
from maus 
)
  select
  nw.month,
  -- vi.buyer_segment, 
  vi.top_channel,
  vi.browser_platform,
  vi.region,
  vi.signed_in,
  count(nw.mapped_user_id) as maus, 
  count(case when nw.next_visit_month = date_add(month, interval 1 month) then mapped_user_id end) as retained,
  count(case when nw.next_visit_month = date_add(month, interval 1 month) then mapped_user_id end) / count(*) as pct_retained,
  sum(gms) as gms
from 
  next_visit_month nw 
left join 
  common_visit_info vi
    using (mapped_user_id)
group by all 
); 
