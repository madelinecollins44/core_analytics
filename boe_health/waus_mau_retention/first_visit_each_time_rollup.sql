
--owner: madelinecollins@etsy.com
--owner_team: product-asf@etsy.com
--description: rollups to measure boe user weekly + monthly retention

begin

--grabs signed in boe visit info from last 2.5 years 
create or replace temp table visits as (
  select 
    v._date
    , date_trunc(v._date, week(MONDAY)) as week
    , date_trunc(v._date, month) as month
    , m.mapped_user_id 
    , v.top_channel
    , v.browser_platform
    , v.region
    , v.visit_id
    , v.total_gms
    , row_number() over (partition by m.mapped_user_id, date_trunc(v._date, week(MONDAY)) order by _date desc) as visit_number_week
    , row_number() over (partition by m.mapped_user_id, date_trunc(v._date, month) order by _date desc) as visit_number_month
  from etsy-data-warehouse-prod.weblog.visits v
  join etsy-data-warehouse-prod.user_mart.mapped_user_profile m using (user_id)
  where 
    platform in ('boe') 
    and _date >= current_date-880 
    and v.user_id is not null ) ;

--grabs most recent visit info from each week/month
create or replace temp table first_of_week as (
  select 
    mapped_user_id
    , week
    , _date
    , top_channel
    , browser_platform
    , region
  from visits
where visit_number_week = 1 
group by all 
);

create or replace temp table first_of_month as (
  select 
    mapped_user_id
    , month
    , _date
    , top_channel
    , browser_platform
    , region
  from visits
where visit_number_month = 1 
group by all 
);

--only need dates that correlate to the first visit in that week/ month 
create or replace temp table combine_dates as (
 with _dates as (
select 
    mapped_user_id
    , _date
  from first_of_month
union all 
  select 
    mapped_user_id
    , _date
  from first_of_week
 )
 select distinct _date, mapped_user_id from _dates
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
      combine_dates ex -- visits in the last two, for yoy calcs
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
  select
    mapped_user_id, 
    _date,
    date_trunc(_date, week(MONDAY)) as week,
    date_trunc(_date, month) as month,
    row_number() over (partition by mapped_user_id, date_trunc(_date, week(MONDAY)) order by _date desc) as number_week,
    row_number() over (partition by mapped_user_id, date_trunc(_date, month) order by _date desc) as number_month,
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
  group by all
); 


--avoid dupes by taking the first buyer segment from each week/ month 
 create or replace temp table buyer_segment_week as (
select 
  mapped_user_id, week, buyer_segment 
from 
  buyer_segment
where number_week =1 
 );

create or replace temp table buyer_segment_month as (
select 
  mapped_user_id, month, buyer_segment 
from 
  buyer_segment
where number_month =1 
 );

create or replace table `etsy-data-warehouse-dev.rollups.boe_waus_retention` as (
with waus as (
  select 
    week,
    mapped_user_id,
    count(distinct visit_id) as visits,
    sum(total_gms) as gms,
  from 
   visits 
  group by all)
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
  nw.mapped_user_id,
  count(nw.mapped_user_id) as waus, 
  count(case when nw.next_visit_week = date_add(nw.week, interval 1 week) then nw.mapped_user_id end) as retained,
  sum(gms) as gms
from 
  next_visit_week nw 
group by all 
union all ----union here 
  select
  'ly' as era,
  date_add(nw.week, interval 52 week) as week,
   nw.mapped_user_id,
  count(nw.mapped_user_id) as waus, 
  count(case when nw.next_visit_week = date_add(nw.week, interval 1 week) then nw.mapped_user_id end) as retained,
  sum(gms) as gms
from 
  next_visit_week nw 
where date_add(nw.week, interval 52 week) <= current_date-1 
group by all 
)
  select
  u.week,
  bs.buyer_segment, 
  rw.top_channel,
  rw.browser_platform,
  rw.region,
  sum(case when era = 'ty' then waus end) AS ty_waus,
  sum(case when era = 'ty' then retained end) AS ty_retained,
  sum(case when era = 'ly' then waus end) AS ly_waus,
  sum(case when era = 'ly' then retained end) AS ly_retained,
from yy_union u
left join first_of_week rw 
  using (mapped_user_id, week)
left join buyer_segment_week bs
  on u.mapped_user_id=bs.mapped_user_id
  and u.week=bs.week
group by all
);

create or replace table `etsy-data-warehouse-dev.rollups.boe_maus_retention` as (
with maus as (
  select 
    month,
    mapped_user_id,
    count(distinct visit_id) as visits,
    sum(total_gms) as gms,
  from 
   visits 
  group by all)
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
  nw.mapped_user_id,
  count(nw.mapped_user_id) as maus, 
  count(case when nw.next_visit_month = date_add(nw.month, interval 1 month) then nw.mapped_user_id end) as retained,
  sum(gms) as gms
from 
  next_visit_month nw 
group by all 
union all ----union here 
  select
  'ly' as era,
  date_add(nw.month, interval 52 month) as month,
   nw.mapped_user_id,
  count(nw.mapped_user_id) as maus, 
  count(case when nw.next_visit_month = date_add(nw.month, interval 1 month) then nw.mapped_user_id end) as retained,
  sum(gms) as gms
from 
  next_visit_month nw 
where date_add(nw.month, interval 52 month) <= current_date-1 
group by all 
)
  select
  u.month,
  bs.buyer_segment, 
  rw.top_channel,
  rw.browser_platform,
  rw.region,
  sum(case when era = 'ty' then maus end) AS ty_maus,
  sum(case when era = 'ty' then retained end) AS ty_retained,
  sum(case when era = 'ly' then maus end) AS ly_maus,
  sum(case when era = 'ly' then retained end) AS ly_retained,
from yy_union u
left join first_of_month rw 
  using (mapped_user_id, month)
left join buyer_segment_month bs
  on u.mapped_user_id=bs.mapped_user_id
  and u.month=bs.month
group by all
);

end 
