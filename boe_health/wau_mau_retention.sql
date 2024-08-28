begin

create or replace temp table visits as (
  select 
    v._date
    , m.mapped_user_id 
    , v.top_channel
    , v.browser_platform
    , v.region
    , m.buyer_segment -- would it be incorrect to grab buyer segment from here? 
    , v.visit_id
    , v.total_gms
  from etsy-data-warehouse-prod.weblog.visits v
  join etsy-data-warehouse-prod.user_mart.mapped_user_profile m using (user_id)
  where 
    platform in ('boe') 
    and _date >= current_date-880 
    and v.user_id is not null) ;

create or replace temp table most_common_info as (
  select 
    mapped_user_id
  , approx_top_count(top_channel, 1)[offset(0)].value as top_channel  
  , approx_top_count(browser_platform, 1)[offset(0)].value as browser_platform
  , approx_top_count(region, 1)[offset(0)].value as region
  , approx_top_count(buyer_segment, 1)[offset(0)].value as buyer_segment
  from visits
group by all 
);

create or replace table `etsy-data-warehouse-dev.rollups.boe_waus_retention` as (
with waus as (
  select 
    date_trunc(v._date, week) as week,
    mapped_user_id,
    count(distinct visit_id) as visits,
    sum(total_gms) as gms,
  from 
   visits v 
  group by all
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
, yy_union as (
  select
  'ty' as era,
  nw.week,
  vi.buyer_segment, 
  vi.top_channel,
  vi.browser_platform,
  vi.region,
  count(nw.mapped_user_id) as waus, 
  count(case when nw.next_visit_week = date_add(week, interval 1 week) then mapped_user_id end) as retained,
  sum(gms) as gms
from 
  next_visit_week nw 
left join 
  most_common_info vi
    using (mapped_user_id)
group by all 
union all ----union here 
  select
  'ly' as era,
  date_add(nw.week, interval 52 week) as week,
  vi.buyer_segment, 
  vi.top_channel,
  vi.browser_platform,
  vi.region,
  count(nw.mapped_user_id) as waus, 
  count(case when nw.next_visit_week = date_add(week, interval 1 week) then mapped_user_id end) as retained,
  sum(gms) as gms
from 
  next_visit_week nw 
left join 
  most_common_info vi
    using (mapped_user_id)
where date_add(nw.week, interval 52 week) <= current_date-1 
group by all 
)
select
  week,
  buyer_segment, 
  top_channel,
  browser_platform,
  region,
  sum(case when era = 'ty' then waus end) AS ty_waus,
  sum(case when era = 'ty' then retained end) AS ty_retained,
  sum(case when era = 'ly' then waus end) AS ly_waus,
  sum(case when era = 'ly' then retained end) AS ly_retained,
from yy_union 
group by all
); 

create or replace table `etsy-data-warehouse-dev.rollups.boe_maus_retention` as (
with maus as (
  select 
    date_trunc(v._date, month) as month,
    mapped_user_id,
    count(distinct visit_id) as visits,
    sum(total_gms) as gms,
  from 
    visits v 
  group by all
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
, yy_union as (
  select
  'ty' as era,
  nw.month,
  vi.buyer_segment, 
  vi.top_channel,
  vi.browser_platform,
  vi.region,
  count(nw.mapped_user_id) as maus, 
  count(case when nw.next_visit_month = date_add(month, interval 1 month) then mapped_user_id end) as retained,
  sum(gms) as gms
from 
  next_visit_month nw 
left join 
  most_common_info vi
    using (mapped_user_id)
group by all 
union all ----union here 
  select
  'ly' as era,
  date_add(nw.month, interval 52 month) as month,
  vi.buyer_segment, 
  vi.top_channel,
  vi.browser_platform,
  vi.region,
  count(nw.mapped_user_id) as maus, 
  count(case when nw.next_visit_month = date_add(month, interval 1 month) then mapped_user_id end) as retained,
  sum(gms) as gms
from 
  next_visit_month nw 
left join 
  most_common_info vi
    using (mapped_user_id)
where date_add(nw.month, interval 52 month) <= current_date-1 
group by all 
)
select
  month,
  buyer_segment, 
  top_channel,
  browser_platform,
  region,
  sum(case when era = 'ty' then maus end) AS ty_maus,
  sum(case when era = 'ty' then retained end) AS ty_retained,
  sum(case when era = 'ly' then maus end) AS ly_maus,
  sum(case when era = 'ly' then retained end) AS ly_retained,
from yy_union 
group by all
); 
end 
