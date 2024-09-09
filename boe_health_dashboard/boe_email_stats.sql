create or replace table `etsy-data-warehouse-dev.madelinecollins.boe_email_stats`  as (
-- users that have visited boe in last year 
with boe_users as (
  -- select 
  --   distinct user_id,
  -- from etsy-data-warehouse-prod.buyer360.buyer_ltd
  -- where boe_app_visits > 0
select
  distinct user_id
  from etsy-data-warehouse-prod.weblog.visits
  where platform in ('boe')
  and _date >= current_date-365 
)
, email_subscriptions as (
select
  a.utm_source
  , count(distinct b.user_id) as boe_subscribers
  , count(distinct a.user_id) as total_subscribers
from 
  etsy-data-warehouse-prod.rollups.email_subscribers a
left join 
  boe_users b using (user_id)
group by all 
)
, email_engagement as (
select 
 d.utm_source
  , count(distinct d.user_id) as users_delivered
  , count(distinct o.user_id) as users_opens
  , count(distinct c.user_id) as users_clicks
  , count(distinct d.euid) as delivered
  , count(distinct o.euid) as opens
  , count(distinct c.euid) as clicks
  , 
from 
  boe_users u
left join 
  etsy-data-warehouse-prod.mail_mart.delivered d using (user_id) 
left join 
  etsy-data-warehouse-prod.mail_mart.opens o
    on d.user_id = o.user_id
    and d.euid=o.euid
left join etsy-data-warehouse-prod.mail_mart.clicks c
    on c.user_id = o.user_id
    and c.euid=o.euid
where date(timestamp_seconds(d.delivered_date)) >= current_date-365
group by all
)
  select 
    a.utm_source
    , b.boe_subscribers 
    , b.total_subscribers
    , a.users_delivered
    , a.users_opens
    , a.users_clicks 
    , a.delivered
    , a.opens
    , a.clicks 
    , u.total_boe_users
from email_engagement a
left join email_subscriptions b using (utm_source)
cross join (
    select count(distinct user_id) AS total_boe_users
    from boe_users) u
);
