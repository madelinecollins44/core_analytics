------------------------------------------------------------------------------------------------------------------------
--USING VISITS W SEGMENTS: this way is wayyyy easier but slightly different buyer segments 
------------------------------------------------------------------------------------------------------------------------  
create or replace temp table first_visits as (
  select
  v.browser_id,
  v.browser_platform,
  v.region,
  v._date as first_app_visit,
  v.user_id,
  -- coalesce(cast(v.user_id as string), v.browser_id) as unique_id, -- coalescing here so even if user is signed out, user_id = null, then browser will be counted 
  s.buyer_segment,
  v.event_source,
  v.start_datetime,
  case when v.user_id is not null then 1 else 0 end as is_signed_in,
  lead(v._date) over (partition by v.browser_id order by v.start_datetime asc) as next_visit_date
from 
  `etsy-data-warehouse-prod.weblog.visits` v  
left join 
    etsy-data-warehouse-prod.rollups.visits_w_segments s using (user_id, visit_id)
  where v.platform = "boe"
  and v._date is not null 
  and v.event_source in ("ios", "android")
  group by all
qualify row_number() over(partition by v.browser_id order by start_datetime desc) = 1
);

create or replace table etsy-data-warehouse-dev.rollups.boe_user_retention as (
select 
is_signed_in,
first_app_visit,
browser_platform,
region,
buyer_segment,--segment when they downloaded the app
count(distinct browser_id) as first_visit,
count(distinct case when first_app_visit = next_visit_date then browser_id end) as same_day_visits,
count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end) as first_7_days,
count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end) as first_14_days,
count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end) as first_30_days,
--pct
count(distinct case when first_app_visit = next_visit_date then browser_id end)/nullif(count(distinct browser_id),0) as pct_same_day_visits,
count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end)/nullif(count(distinct browser_id),0) as pct_first_7_days,
count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end)/nullif(count(distinct browser_id),0) as pct_first_14_days,
count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end)/nullif(count(distinct browser_id),0) as pct_first_30_days
from first_visits
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
  count(distinct browser_id) as first_visit,
  count(distinct case when first_app_visit = next_visit_date then browser_id end) as same_day_visits,
  count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end) as first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end) as first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end) as first_30_days,
  --pct
  count(distinct case when first_app_visit = next_visit_date then browser_id end)/nullif(count(distinct browser_id),0) as pct_same_day_visits,
  count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end)/nullif(count(distinct browser_id),0) as pct_first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end)/nullif(count(distinct browser_id),0) as pct_first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end)/nullif(count(distinct browser_id),0) as pct_first_30_days
from first_visits
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
  count(distinct browser_id) as first_visit,
  count(distinct case when first_app_visit = next_visit_date then browser_id end) as same_day_visits,
  count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end) as first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end) as first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end) as first_30_days,
  --pct
  count(distinct case when first_app_visit = next_visit_date then browser_id end)/nullif(count(distinct browser_id),0) as pct_same_day_visits,
  count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end)/nullif(count(distinct browser_id),0) as pct_first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end)/nullif(count(distinct browser_id),0) as pct_first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end)/nullif(count(distinct browser_id),0) as pct_first_30_days
from first_visits
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
  sum(CASE WHEN era = 'ty' THEN first_visit END) AS ty_first_visit,
  sum(CASE WHEN era = 'ty' THEN first_7_days END) AS ty_first_7_days,
  sum(CASE WHEN era = 'ty' THEN first_14_days END) AS ty_first_14_days,
  sum(CASE WHEN era = 'ty' THEN first_30_days END) AS ty_first_30_days,
  sum(CASE WHEN era = 'ty' THEN pct_same_day_visits END) AS ty_pct_same_day_visits,
  sum(CASE WHEN era = 'ty' THEN pct_first_7_days END) AS ty_pct_first_7_days,
  sum(CASE WHEN era = 'ty' THEN pct_first_14_days END) AS ty_pct_first_14_days,
  sum(CASE WHEN era = 'ty' THEN pct_first_30_days END) AS ty_pct_first_30_days,
  --ly metrics
  sum(CASE WHEN era = 'ly' THEN first_visit END) AS ly_first_visit,
  sum(CASE WHEN era = 'ly' THEN first_7_days END) AS ly_first_7_days,
  sum(CASE WHEN era = 'ly' THEN first_14_days END) AS ly_first_14_days,
  sum(CASE WHEN era = 'ly' THEN first_30_days END) AS ly_first_30_days,
  sum(CASE WHEN era = 'ly' THEN pct_same_day_visits END) AS ly_pct_same_day_visits,
  sum(CASE WHEN era = 'ly' THEN pct_first_7_days END) AS ly_pct_first_7_days,
  sum(CASE WHEN era = 'ly' THEN pct_first_14_days END) AS ly_pct_first_14_days,
  sum(CASE WHEN era = 'ly' THEN pct_first_30_days END) AS ly_pct_first_30_days,
  sum(CASE WHEN era = 'ly' THEN pct_first_7_days END) AS ly_pct_first_7_days,
  FROM
    yoy_union
  WHERE first_app_visit < CAST(current_date() as DATETIME)
  GROUP BY all
);

------------------------------------------------------------------------------------------------------------------------
--USING MAPPED USER ID
------------------------------------------------------------------------------------------------------------------------
----this rollup looks at the first visit for each user. if the user is signed out, then we use the browser_id
  begin

create or replace temp table first_visits as (
  select
  v.browser_id,
  v.browser_platform,
  v.region,
  v._date as first_app_visit,
  v.user_id,
  coalesce(cast(v.user_id as string), v.browser_id) as unique_id, -- coalescing here so even if user is signed out, user_id = null, then browser will be counted 
  s.buyer_segment,
  v.event_source,
  v.start_datetime,
  case when v.user_id is not null then 1 else 0 end as is_signed_in,
  lead(v._date) over (partition by coalesce(cast(v.user_id as string), v.browser_id) order by v.start_datetime asc) as next_visit_date
from 
  `etsy-data-warehouse-prod.weblog.visits` v  
left join 
    etsy-data-warehouse-prod.user_mart.mapped_user_profile s using (user_id)
  where v.platform = "boe"
  and v._date is not null 
  -- and v._date >= current_date-730
  and v.event_source in ("ios", "android")
  group by all
qualify row_number() over(partition by coalesce(cast(v.user_id as string), v.browser_id) order by start_datetime desc) = 1
);

--looks at user retention by segments
create or replace table etsy-data-warehouse-dev.rollups.boe_user_retention as (
select 
is_signed_in,
browser_platform,
region,
buyer_segment,--segment when they downloaded the app
-- agg totals for unique_id, again this is so signed out users are still counted 
count(distinct unique_id) as first_visit,
count(distinct case when first_app_visit = next_visit_date then unique_id end) as same_day_visits,
count(distinct case when next_visit_date <= first_app_visit + 6 then unique_id end) as first_7_days,
count(distinct case when next_visit_date <= first_app_visit + 13 then unique_id end) as first_14_days,
count(distinct case when next_visit_date <= first_app_visit + 29 then unique_id end) as first_30_days,
--pct
count(distinct case when first_app_visit = next_visit_date then unique_id end)/nullif(count(distinct unique_id),0) as pct_same_day_visits,
count(distinct case when next_visit_date <= first_app_visit + 6 then unique_id end)/nullif(count(distinct unique_id),0) as pct_first_7_days,
count(distinct case when next_visit_date <= first_app_visit + 13 then unique_id end)/nullif(count(distinct unique_id),0) as pct_first_14_days,
count(distinct case when next_visit_date <= first_app_visit + 29 then unique_id end)/nullif(count(distinct unique_id),0) as pct_first_30_days
from first_visits
where first_app_visit <= current_date
group by all
);

--looks at user retention by segments, yoy comparison
create or replace table etsy-data-warehouse-dev.rollups.boe_user_retention_yoy as (
with yoy_union as (
select 
  'ty' as era,
  first_app_visit,
  is_signed_in,
  browser_platform,
  region,
  buyer_segment,--segment when they downloaded the app
  -- agg totals for unique_id, again this is so signed out users are still counted 
  count(distinct unique_id) as first_visit,
  count(distinct case when first_app_visit = next_visit_date then unique_id end) as same_day_visits,
  count(distinct case when next_visit_date <= first_app_visit + 6 then unique_id end) as first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then unique_id end) as first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then unique_id end) as first_30_days,
  --pct
  count(distinct case when first_app_visit = next_visit_date then unique_id end)/nullif(count(distinct unique_id),0) as pct_same_day_visits,
  count(distinct case when next_visit_date <= first_app_visit + 6 then unique_id end)/nullif(count(distinct unique_id),0) as pct_first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then unique_id end)/nullif(count(distinct unique_id),0) as pct_first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then unique_id end)/nullif(count(distinct unique_id),0) as pct_first_30_days
from first_visits
union all
select
  'ly' as era,
  CAST(date_add(first_app_visit, interval 52 WEEK) as DATETIME) AS first_app_visit,
  is_signed_in,
  browser_platform,
  region,
  buyer_segment,--segment when they downloaded the app
  -- agg totals for unique_id, again this is so signed out users are still counted 
  count(distinct unique_id) as first_visit,
  count(distinct case when first_app_visit = next_visit_date then unique_id end) as same_day_visits,
  count(distinct case when next_visit_date <= first_app_visit + 6 then unique_id end) as first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then unique_id end) as first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then unique_id end) as first_30_days,
  --pct
  count(distinct case when first_app_visit = next_visit_date then unique_id end)/nullif(count(distinct unique_id),0) as pct_same_day_visits,
  count(distinct case when next_visit_date <= first_app_visit + 6 then unique_id end)/nullif(count(distinct unique_id),0) as pct_first_7_days,
  count(distinct case when next_visit_date <= first_app_visit + 13 then unique_id end)/nullif(count(distinct unique_id),0) as pct_first_14_days,
  count(distinct case when next_visit_date <= first_app_visit + 29 then unique_id end)/nullif(count(distinct unique_id),0) as pct_first_30_days
from first_visits
)
SELECT
  era,
  first_app_visit,
  is_signed_in,
  browser_platform,
  region,
  buyer_segment,
  --ty metrics
  sum(CASE WHEN era = 'ty' THEN first_visit END) AS ty_first_visit,
  sum(CASE WHEN era = 'ty' THEN first_7_days END) AS ty_first_7_days,
  sum(CASE WHEN era = 'ty' THEN first_14_days END) AS ty_first_14_days,
  sum(CASE WHEN era = 'ty' THEN first_30_days END) AS ty_first_30_days,
  sum(CASE WHEN era = 'ty' THEN pct_next_day_visits END) AS ty_pct_same_day_visits,
  sum(CASE WHEN era = 'ty' THEN pct_first_7_days END) AS ty_pct_first_7_days,
  sum(CASE WHEN era = 'ty' THEN pct_first_14_days END) AS ty_pct_first_14_days,
  sum(CASE WHEN era = 'ty' THEN pct_first_30_days END) AS ty_pct_first_30_days,
  sum(CASE WHEN era = 'ty' THEN pct_first_7_days END) AS ty_pct_first_7_days,
  --ly metrics
  sum(CASE WHEN era = 'ly' THEN first_visit END) AS ly_first_visit,
  sum(CASE WHEN era = 'ly' THEN first_7_days END) AS ly_first_7_days,
  sum(CASE WHEN era = 'ly' THEN first_14_days END) AS ly_first_14_days,
  sum(CASE WHEN era = 'ly' THEN first_30_days END) AS ly_first_30_days,
  sum(CASE WHEN era = 'ly' THEN pct_next_day_visits END) AS ly_pct_same_day_visits,
  sum(CASE WHEN era = 'ly' THEN pct_first_7_days END) AS ly_pct_first_7_days,
  sum(CASE WHEN era = 'ly' THEN pct_first_14_days END) AS ly_pct_first_14_days,
  sum(CASE WHEN era = 'ly' THEN pct_first_30_days END) AS ly_pct_first_30_days,
  sum(CASE WHEN era = 'ly' THEN pct_first_7_days END) AS ly_pct_first_7_days,
  FROM
    yoy_union
  WHERE first_app_visit < CAST(current_date() as DATETIME)
  GROUP BY all
);

end
______________________________________________________________________________________________________________________________________________________________________________________
-----testing for method above
with first_visits as (
  select
  v.browser_id,
  v.browser_platform,
  v.region,
  v._date as first_app_visit,
  v.user_id,
  coalesce(cast(v.user_id as string), v.browser_id) as unique_id, -- coalescing here so even if user is signed out, user_id = null, then browser will be counted 
  s.buyer_segment,
  v.event_source,
  v.start_datetime,
  case when v.user_id is not null then 1 else 0 end as is_signed_in,
  lead(v._date) over (partition by v.user_id order by v.start_datetime asc) as next_visit_date
from 
  `etsy-data-warehouse-prod.weblog.visits` v  
left join 
    etsy-data-warehouse-prod.user_mart.mapped_user_profile s using (user_id)
  where v.platform = "boe"
  and v._date is not null 
  and v._date >= current_date-730
  and v.event_source in ("ios", "android")
  group by all
qualify row_number() over(partition by v.user_id order by start_datetime desc) = 1
)
select count(distinct case when user_id is null then browser_id end), count(distinct user_id) from first_visits group by all 
  --only 1 signed out user???
______________________________________________________________________________________________________________________________________________________________________________________
-- create or replace table etsy-data-warehouse-dev.rollups.boe_user_retention as (
-- with first_visits as (
--   select
--   v.browser_id,
--   v.browser_platform,
--   v.region,
--   v._date as first_app_visit,
--   v.user_id,
--   s.buyer_segment,
--   v.event_source,
--   v.start_datetime,
--   case when v.user_id is not null then 1 else 0 end as is_signed_in,
--   lead(v._date) over (partition by v.browser_id order by v.start_datetime asc) as next_visit_date
-- from 
--   `etsy-data-warehouse-prod.weblog.visits` v  
-- left join 
--     etsy-data-warehouse-prod.user_mart.mapped_user_profile s using (user_id)
--   where v.platform = "boe"
--   and v._date is not null 
--   and v._date >= current_date-730
--   and v.event_source in ("ios", "android")
--   group by all
-- qualify row_number() over(partition by v.browser_id order by start_datetime desc) = 1
-- )
-- select 
-- is_signed_in,
-- browser_platform,
-- region,
-- buyer_segment,--segment when they downloaded the app
-- -- agg totals for browser
-- count(distinct case when first_app_visit = next_visit_date then browser_id end) as next_day_visits_browser,
-- count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end) as first_7_days_browser,
-- count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end) as first_14_days_browser,
-- count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end) as first_30_days_browser,
-- -- agg totals for user
-- count(distinct case when first_app_visit = next_visit_date then user_id end) as next_day_visits_user,
-- count(distinct case when next_visit_date <= first_app_visit + 6 then user_id end) as first_7_days_user,
-- count(distinct case when next_visit_date <= first_app_visit + 13 then user_id end) as first_14_days_user,
-- count(distinct case when next_visit_date <= first_app_visit + 29 then user_id end) as first_30_days_user,
-- --pct
-- -- count(distinct case when first_app_visit = next_visit_date then browser_id end)/count(distinct browser_id) as pct_next_day_visits,
-- -- count(distinct case when next_visit_date <= first_app_visit + 6 then browser_id end)/count(distinct browser_id) as pct_first_7_days,
-- -- count(distinct case when next_visit_date <= first_app_visit + 13 then browser_id end)/count(distinct browser_id) as pct_first_14_days,
-- -- count(distinct case when next_visit_date <= first_app_visit + 29 then browser_id end)/count(distinct browser_id) as pct_first_30_days
-- from first_visits
-- where first_app_visit <= current_date
-- group by all
-- );
