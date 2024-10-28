----------------------------------------------------------------------------------------------------
--What share of visits & GMS flow through Shop Home today?  Overall and by platform
----include YY comparison
----------------------------------------------------------------------------------------------------
----find most popular pages in last 30 days 
select 
  event_type
  , count(distinct visit_id) as visits
from 
  etsy-data-warehouse-prod.weblog.events
where 
   _date>= current_date-30
   and page_view =1 
group by all
order by 2 desc

  
----compare to most popular pages
with event_visits as (
select distinct
  visit_id,
  event_type
from 
  etsy-data-warehouse-prod.weblog.events
where 
  event_type in ('view_listing','shop_home','cart_view','home','homescreen','thank_you','search','your_purchases')
  and _date>= current_date-30
)
select
  -- v._date, 
  count(distinct v.visit_id) as total_visits, 
  count(distinct case when event_type in ('view_listing') then ev.visit_id end) as view_listing_visits, 
  count(distinct case when event_type in ('shop_home') then ev.visit_id end) as shop_home_visits, 
  count(distinct case when event_type in ('cart_view') then ev.visit_id end) as cart_view_visits, 
  count(distinct case when event_type in ('home','homescreen') then ev.visit_id end) as home_visits, 
  count(distinct case when event_type in ('thank_you') then ev.visit_id end) as thank_you_visits, 
  count(distinct case when event_type in ('search') then ev.visit_id end) as search_visits, 
  count(distinct case when event_type in ('your_purchases') then ev.visit_id end) as your_purchases_visits, 

  sum(v.total_gms) as total_gms, 
  sum(case when event_type in ('view_listing') and ev.visit_id is not null then v.total_gms end) as view_listing_gms, 
  sum(case when event_type in ('shop_home') and ev.visit_id is not null then v.total_gms end) as shop_home_gms, 
  sum(case when event_type in ('cart_view') and ev.visit_id is not null then v.total_gms end) as cart_view_gms, 
  sum(case when event_type in ('home','homescreen') and ev.visit_id is not null then v.total_gms end) as home_gms, 
  sum(case when event_type in ('thank_you') and ev.visit_id is not null then v.total_gms end) as thank_you_gms, 
  sum(case when event_type in ('search') and ev.visit_id is not null then v.total_gms end) as search_gms, 
  sum(case when event_type in ('your_purchases') and ev.visit_id is not null then v.total_gms end) as your_purchases_gms, 
from 
  etsy-data-warehouse-prod.weblog.visits v
left join event_visits ev using (visit_id)
where v._date >= current_date-30
group by all 

----look at agg over last 30 days, platform
with shop_home_visits as (
select distinct
  visit_id
from 
  etsy-data-warehouse-prod.weblog.events
where 
  event_type in ('shop_home')
  and _date>= current_date-30
)
select
  -- v._date, 
  -- v.platform,
  count(distinct v.visit_id) as total_visits, 
  count(distinct shv.visit_id) as shop_home_visits, 
  count(distinct shv.visit_id)/count(distinct v.visit_id) as share_of_visits, 
  sum(v.total_gms) as total_gms, 
  sum(case when shv.visit_id is not null then v.total_gms end) as shop_home_gms, 
  sum(case when shv.visit_id is not null then v.total_gms end)/sum(v.total_gms) as share_of_gms
from 
  etsy-data-warehouse-prod.weblog.visits v
left join 
  shop_home_visits shv using (visit_id)
where v._date >= current_date-30
group by all 

---------------------------------------------------------------------------------------------------------------------------------------------
--Which types of buyers go to shop home? 
----Buyer segment, visit channel, platform, past 7d visits, X listing views in session, engaged visits, signed in vs signed out, left reviews
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
--Where do they go after shop home?  
----Next screen, segment by visitors that purchase in-session vs. not, purchase something from the shop vs. not
---------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------
--What are the most used parts of the page? 
----Scroll depth, clicks, etc
----Segment by visitors that purchase in-session vs. not, purchase something from the shop vs. not
---------------------------------------------------------------------------------------------------------------------------------------------
