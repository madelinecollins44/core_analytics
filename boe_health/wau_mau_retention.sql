begin

create or replace temp table visits as (
  select 
    v._date
    , m.mapped_user_id 
    , v.top_channel
    , v.browser_platform
    , v.region
    -- , m.buyer_segment -- would it be incorrect to grab buyer segment from here? 
    , v.visit_id
    , v.total_gms
  from etsy-data-warehouse-prod.weblog.visits v
  join etsy-data-warehouse-prod.user_mart.mapped_user_profile m using (user_id)
  where 
    platform in ('boe') 
    and _date >= current_date-880 
    and v.user_id is not null) ;

create or replace temp table user_date as (
  select distinct
    v._date
    , mapped_user_id 
  from visits v) ;

create or replace temp table most_common_info as (
  select 
    mapped_user_id
  , approx_top_count(top_channel, 1)[offset(0)].value as top_channel  
  , approx_top_count(browser_platform, 1)[offset(0)].value as browser_platform
  , approx_top_count(region, 1)[offset(0)].value as region
  -- , approx_top_count(buyer_segment, 1)[offset(0)].value as buyer_segment
  from visits
group by all 
);

create or replace temp table buyer_segment as (
with purchase_info as (
  select
      a.mapped_user_id, 
      ex._date, 
      min(date) AS first_purchase_date, 
      max(date) AS last_purchase_date,
      coalesce(sum(gms_net),0) AS lifetime_gms,
      coalesce(count(DISTINCT date),0) AS lifetime_purchase_days, 
      coalesce(count(DISTINCT receipt_id),0) AS lifetime_orders,
      round(cast(round(coalesce(sum(CASE WHEN date between date_sub(_date, interval 365 DAY) and _date THEN gms_net END), CAST(0 as NUMERIC)),20) as numeric),2) AS past_year_gms,
      count(DISTINCT CASE WHEN date between date_sub(_date, interval 365 DAY) and _date THEN date END) AS past_year_purchase_days,
      count(DISTINCT CASE WHEN date between date_sub(_date, interval 365 DAY) and _date THEN receipt_id END) AS past_year_orders
    from 
      `etsy-data-warehouse-prod.user_mart.mapped_user_profile` a
    join
      user_date ex -- visits in the last two, for yoy calcs
        ON ex.mapped_user_id = a.mapped_user_id
    join 
      `etsy-data-warehouse-prod.user_mart.user_mapping` b
        on a.mapped_user_id = b.mapped_user_id
    join 
      `etsy-data-warehouse-prod.user_mart.user_first_visits` c
        on b.user_id = c.user_id
    left join 
      `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` e
        on a.mapped_user_id = e.mapped_user_id 
        and e.date <= ex._date-1 and market <> 'ipp'
    GROUP BY all
    having (ex._date >= min(date(timestamp_seconds(a.join_date))) or ex._date >= min(date(c.start_datetime)))
  )
  , all_segments as (
  select
    mapped_user_id, 
    _date,
    CASE  
      when p.lifetime_purchase_days = 0 or p.lifetime_purchase_days is null then 'Zero Time'  
      when date_diff(_date, p.first_purchase_date, DAY)<=180 and (p.lifetime_purchase_days=2 or round(cast(round(p.lifetime_gms,20) as numeric),2) >100.00) then 'High Potential' 
      WHEN p.lifetime_purchase_days = 1 and date_diff(_date, p.first_purchase_date, DAY) <=365 then 'OTB'
      when p.past_year_purchase_days >= 6 and p.past_year_gms >=200 then 'Habitual' 
      when p.past_year_purchase_days>=2 then 'Repeat' 
      when date_diff(_date , p.last_purchase_date, DAY) >365 then 'Lapsed'
      else 'Active' 
      end as buyer_segment,
  from purchase_info p
  group by all)
select 
  mapped_user_id
  , approx_top_count(buyer_segment, 1)[offset(0)].value as buyer_segment
from  all_segments 
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
  b.buyer_segment, 
  vi.top_channel,
  vi.browser_platform,
  vi.region,
  count(nw.mapped_user_id) as waus, 
  count(case when nw.next_visit_week = date_add(week, interval 1 week) then nw.mapped_user_id end) as retained,
  sum(gms) as gms
from 
  next_visit_week nw 
left join 
  most_common_info vi
    using (mapped_user_id)
left join buyer_segment b on nw.mapped_user_id=b.mapped_user_id 
group by all 
union all ----union here 
  select
  'ly' as era,
  date_add(nw.week, interval 52 week) as week,
  b.buyer_segment, 
  vi.top_channel,
  vi.browser_platform,
  vi.region,
  count(nw.mapped_user_id) as waus, 
  count(case when nw.next_visit_week = date_add(week, interval 1 week) then nw.mapped_user_id end) as retained,
  sum(gms) as gms
from 
  next_visit_week nw 
left join 
  most_common_info vi
    using (mapped_user_id)
left join buyer_segment b on nw.mapped_user_id=b.mapped_user_id and b.week=nw.week
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
  count(case when nw.next_visit_month = date_add(month, interval 1 month) then nw.mapped_user_id end) as retained,
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
  count(case when nw.next_visit_month = date_add(month, interval 1 month) then nw.mapped_user_id end) as retained,
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
