begin 

create or replace temp table first_visits as (
  select
  v.browser_id,
  v.browser_platform,
  v.region,
  v._date as first_app_visit,
  v.user_id,
  v.event_source,
  v.start_datetime,
  u.mapped_user_id,
  case when v.user_id is not null then 1 else 0 end as is_signed_in,
  lead(v._date) over (partition by v.browser_id order by v.start_datetime asc) as next_visit_date
from 
  `etsy-data-warehouse-prod.weblog.visits` v  
left join `etsy-data-warehouse-prod.user_mart.user_mapping` u 
  on v.user_id = u.user_id
where v.platform = "boe"
and v._date >= current_date- 730
and v.event_source in ("ios", "android")
and v.platform in ('boe')
group by all
qualify row_number() over(partition by v.browser_id order by start_datetime) = 1
);

CREATE OR REPLACE TEMP TABLE buyer_segments as (
with purchase_stats as (
  SELECT
      a.mapped_user_id, 
      ex._date, 
      min(date) AS first_purchase_date, 
      max(date) AS last_purchase_date,
      coalesce(sum(gms_net),0) AS lifetime_gms,
      coalesce(count(DISTINCT date),0) AS lifetime_purchase_days, 
      coalesce(count(DISTINCT receipt_id),0) AS lifetime_orders,
      round(cast(round(coalesce(sum(CASE
          WHEN date between date_sub(_date, interval 365 DAY) and _date THEN gms_net
      END), CAST(0 as NUMERIC)),20) as numeric),2) AS past_year_gms,
      count(DISTINCT CASE
          WHEN date between date_sub(_date, interval 365 DAY) and _date THEN date
      END) AS past_year_purchase_days,
      count(DISTINCT CASE
          WHEN date between date_sub(_date, interval 365 DAY) and _date THEN receipt_id
      END) AS past_year_orders
    from 
      `etsy-data-warehouse-prod.user_mart.mapped_user_profile` a
    join
       first_visits ex 
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
    CASE  
      when p.lifetime_purchase_days = 0 or p.lifetime_purchase_days is null then 'Zero Time'  
      when date_diff(_date,p.first_purchase_date, DAY)<=180 and (p.lifetime_purchase_days=2 or round(cast(round(p.lifetime_gms,20) as numeric),2) >100.00) then 'High Potential' 
      WHEN p.lifetime_purchase_days = 1 and date_diff(_date,p.first_purchase_date, DAY) <=365 then 'OTB'
      when p.past_year_purchase_days >= 6 and p.past_year_gms >=200 then 'Habitual' 
      when p.past_year_purchase_days>=2 then 'Repeat' 
      when date_diff(_date , p.last_purchase_date, DAY) >365 then 'Lapsed'
      else 'Active' 
      end as buyer_segment,
  from purchase_stats p
); 

create or replace table etsy-data-warehouse-dev.rollups.boe_user_retention as (
  select 
  first_app_visit,
  is_signed_in,
  browser_platform,
  region,
  buyer_segment, --segment when they downloaded the app
  count(distinct browser_id) as browsers_with_first_visit,
  count(distinct case when first_app_visit = next_visit_date then browser_id end) as browsers_visit_in_same_day,
  count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end) as browsers_visit_in_first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end) as browsers_visit_in_first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end) as browsers_visit_in_first_30_days,
  --pct
  count(distinct case when first_app_visit = next_visit_date then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_same_day,
  count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_first_30_days
  from first_visits
  left join buyer_segments using (mapped_user_id, _date)
  where first_app_visit <= current_date
  group by all
);

create or replace table etsy-data-warehouse-dev.rollups.boe_user_retention_yoy as (
with yoy_union as (
select 
  'ty' as era,
  first_app_visit,
  is_signed_in,
  browser_platform,
  region,
  buyer_segment,--segment when they downloaded the app

  -- agg totals for browser_id, again this is so signed out users are still counted 
  count(distinct browser_id) as browsers_with_first_visit,
  count(distinct case when first_app_visit = next_visit_date then browser_id end) as browsers_visit_in_same_day,
  count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end) as browsers_visit_in_first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end) as browsers_visit_in_first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end) as browsers_visit_in_first_30_days,
    --pct
  count(distinct case when first_app_visit = next_visit_date then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_same_day,
  count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_first_30_days
  
from first_visits
left join buyer_segments using (mapped_user_id, _date)
group by all
union all
select
  'ly' as era,
  CAST(date_add(first_app_visit, interval 52 WEEK) as DATETIME) AS first_app_visit,
  is_signed_in,
  browser_platform,
  region,
  buyer_segment,--segment when they downloaded the app
  -- agg totals for browser_id, again this is so signed out users are still counted 
  count(distinct browser_id) as browsers_with_first_visit,
  count(distinct case when first_app_visit = next_visit_date then browser_id end) as browsers_visit_in_same_day,
  count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end) as browsers_visit_in_first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end) as browsers_visit_in_first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end) as browsers_visit_in_first_30_days,
    --pct
  count(distinct case when first_app_visit = next_visit_date then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_same_day,
  count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end)/nullif(count(distinct browser_id),0) as pct_browsers_visit_in_first_30_days
  from first_visits
  left join buyer_segments using (mapped_user_id)
group by all 
)
SELECT
  era,
  first_app_visit,
  is_signed_in,
  browser_platform,
  region,
  buyer_segment,
  --ty metrics
  sum(CASE WHEN era = 'ty' THEN browsers_with_first_visit END) AS ty_browsers_with_first_visit,
  sum(CASE WHEN era = 'ty' THEN browsers_visit_in_first_7_days END) AS ty_browsers_visit_in_first_7_days,
  sum(CASE WHEN era = 'ty' THEN browsers_visit_in_first_14_days END) AS ty_browsers_visit_in_first_14_days,
  sum(CASE WHEN era = 'ty' THEN browsers_visit_in_first_30_days END) AS ty_browsers_visit_in_first_30_days,
  sum(CASE WHEN era = 'ty' THEN pct_browsers_visit_in_same_day END) AS ty_pct_browsers_visit_in_same_day,
  sum(CASE WHEN era = 'ty' THEN pct_browsers_visit_in_first_7_days END) AS ty_pct_browsers_visit_in_first_7_days,
  sum(CASE WHEN era = 'ty' THEN pct_browsers_visit_in_first_14_days END) AS ty_pct_browsers_visit_in_first_14_days,
  sum(CASE WHEN era = 'ty' THEN pct_browsers_visit_in_first_30_days END) AS ty_pct_browsers_visit_in_first_30_days,
  --ly metrics
  sum(CASE WHEN era = 'ly' THEN browsers_with_first_visit END) AS ly_browsers_with_first_visit,
  sum(CASE WHEN era = 'ly' THEN browsers_visit_in_first_7_days END) AS ly_browsers_visit_in_first_7_days,
  sum(CASE WHEN era = 'ly' THEN browsers_visit_in_first_14_days END) AS ly_browsers_visit_in_first_14_days,
  sum(CASE WHEN era = 'ly' THEN browsers_visit_in_first_30_days END) AS ly_browsers_visit_in_first_30_days,
  sum(CASE WHEN era = 'ly' THEN pct_browsers_visit_in_same_day END) AS ly_pct_browsers_visit_in_same_day,
  sum(CASE WHEN era = 'ly' THEN pct_browsers_visit_in_first_7_days END) AS ly_pct_browsers_visit_in_first_7_days,
  sum(CASE WHEN era = 'ly' THEN pct_browsers_visit_in_first_14_days END) AS ly_pct_browsers_visit_in_first_14_days,
  sum(CASE WHEN era = 'ly' THEN pct_browsers_visit_in_first_30_days END) AS ly_pct_browsers_visit_in_first_30_days,
  FROM
    yoy_union
  WHERE first_app_visit < CAST(current_date() as DATETIME)
  GROUP BY all
);

end 
