------code from https://etsy.slack.com/archives/D063LTSJ1GT/p1722348600396999

-- create temp table to pull buyer segment

begin 

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
       (select _date, user_id from etsy-data-warehouse-prod.weblog.visits where platform in ('boe') and _date >= "2024-06-01" and user_id is not null) ex 
        ON ex.user_id = a.user_id
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
);  


--WAU Retention
create or replace table etsy-data-warehouse-dev.rollups.boe_waus_retention as (
with waus as (
  select 
    date_trunc(_date, week) as week,
    buyer_segment,
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
  left join 
    buyer_segment s --grabs buyer_segment at beginning of week
      on b.mapped_user_id=s.mapped_user_id
      and v._date=s._date
  where platform = "boe"
    and _date >= "2024-06-01"
    and _date != "2024-02-29"
  group by all
  )
-- next, grab the visit info that appears the most for each mapped_user_id. since its tracking user_ids, we cannot track signed out 
, common_visit_info as (
select
  mapped_user_id
  , approx_top_count(top_channel, 1)[offset(0)].value as top_channel  
  , approx_top_count(browser_platform, 1)[offset(0)].value as browser_platform
  , approx_top_count(region, 1)[offset(0)].value as region
  , approx_top_count(buyer_segment, 1)[offset(0)].value as buyer_segment
from waus
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
  select
  nw.week,
  vi.buyer_segment, 
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

-- -- MAU Retention
-- create or replace table etsy-data-warehouse-dev.rollups.boe_maus_retention as (
-- with maus as (
--   select 
--     date_trunc(_date, month) as month,
--     buyer_segment,
--     top_channel,
--     browser_platform,
--     region,
--     case when v.user_id is not null then 1 else 0 end as signed_in,
--     b.mapped_user_id,
--     count(distinct v.visit_id) as visits,
--     sum(v.total_gms) as gms,
--   from 
--     etsy-data-warehouse-prod.weblog.visits v 
--   left join  
--     etsy-data-warehouse-prod.user_mart.user_mapping b 
--       on v.user_id=b.user_id 
--   left join 
--     buyer_segment s --grabs buyer_segment at beginning of month
--       on b.mapped_user_id=s.mapped_user_id
--       and v._date=s._date
--   where platform = "boe"
--     and _date >= "2024-06-01"
--     and _date != "2024-02-29"
--   group by all
--   )
-- -- next, grab the visit info that appears the most for each mapped_user_id. since its tracking user_ids, we cannot track signed out 
-- , common_visit_info as (
-- select
--   mapped_user_id
--   , approx_top_count(top_channel, 1)[offset(0)].value as top_channel  
--   , approx_top_count(browser_platform, 1)[offset(0)].value as browser_platform
--   , approx_top_count(region, 1)[offset(0)].value as region
--   , approx_top_count(buyer_segment, 1)[offset(0)].value as buyer_segment
-- from maus
-- group by all 
-- )
-- --now get the visits from the following month for each mapped user id
-- , next_visit_month as (
-- select  
--   month, 
--   visits, 
--   gms,
--   mapped_user_id, 
--   lead(month) over (partition by mapped_user_id order by month asc) as next_visit_month 
-- from maus 
-- )
--   select
--   nw.month,
--   vi.buyer_segment, 
--   vi.top_channel,
--   vi.browser_platform,
--   vi.region,
--   vi.signed_in,
--   count(nw.mapped_user_id) as maus, 
--   count(case when nw.next_visit_month = date_add(month, interval 1 month) then mapped_user_id end) as retained,
--   count(case when nw.next_visit_month = date_add(month, interval 1 month) then mapped_user_id end) / count(*) as pct_retained,
--   sum(gms) as gms
-- from 
--   next_visit_month nw 
-- left join 
--   common_visit_info vi
--     using (mapped_user_id)
-- group by all 
-- ); 

end
