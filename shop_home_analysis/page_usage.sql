----------------------------------------------------------------------------------------------------
--What share of visits & GMS flow through Shop Home today?  Overall and by platform
----include YY comparison
----------------------------------------------------------------------------------------------------
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
  v._date
  , count(distinct v.visit_id) as total_visits
  , count(distinct shv.visit_id) as shop_home_visits
  , count(distinct shv.visit_id)/count(distinct v.visit_id) as share_of_visits
  , sum(v.total_gms) as total_gms
  , sum(case when shv.visit_id is not null then v.total_gms end) as shop_home_gms
  , sum(case when shv.visit_id is not null then v.total_gms end)/sum(case when shv.visit_id is not null then v.total_gms end) as share_of_gms
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
