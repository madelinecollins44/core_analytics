create or replace table `etsy-data-warehouse-dev.madelinecollins.boe_email_stats`  as (
-- users that have visited boe in last year 
with boe_users as (
  select 
    distinct user_id,
  from etsy-data-warehouse-prod.buyer360.buyer_ltd
  where boe_app_visits > 0
)
, email_subscriptions as (
select
  utm_source
  , count(distinct a.user_id) as boe_subscribers 
  , count(distinct b.user_id) as total_subscribers 
from 
  etsy-data-warehouse-prod.rollups.email_subscribers b
left join 
  boe_users a  using (user_id)
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
from 
  boe_users u
inner join 
  etsy-data-warehouse-prod.mail_mart.delivered d using (user_id) -- only looks at deliveries from users that have visited boe in last year 
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
    , boe_subscribers 
    , total_subscribers 
    , users_delivered
    , users_opens
    , users_clicks 
    , delivered
    , opens
    , clicks 
from email_engagement a
left join email_subscriptions b using (utm_source)
);
------------------------------
create or replace table `etsy-data-warehouse-dev.madelinecollins.boe_email_stats`  as (
-- users that have visited boe in last year 
with boe_users as (
  select 
    distinct user_id,
  from etsy-data-warehouse-prod.buyer360.buyer_ltd
  where boe_app_visits > 0
)
, email_subscriptions as (
select
  utm_source
  , count(distinct a.user_id) as boe_users
  , count(distinct b.user_id) as boe_subscribers
from boe_users a
left join etsy-data-warehouse-prod.rollups.email_subscribers b using (user_id)
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
from 
  boe_users u
inner join 
  etsy-data-warehouse-prod.mail_mart.delivered d using (user_id) -- only looks at deliveries from users that have visited boe in last year 
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
    , boe_subscribers 
    , boe_users 
    , users_delivered
    , users_opens
    , users_clicks 
    , delivered
    , opens
    , clicks 
from email_engagement a
left join email_subscriptions b using (utm_source)
);
