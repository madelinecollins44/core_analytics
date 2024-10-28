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
-- view_listing, 580961218
-- homescreen, 166773892
-- shop_home, 151715149
-- home, 142581209
-- recommended, 110865610
-- market, 98550584
-- listing_page_recommendations, 91328633
--cart_view, 81441882
--search, 79115151
--browselistings, 60008060
-- autosuggest, 51274396
  
----compare to most popular pages
select
  -- v._date, 
  count(distinct v.visit_id) as total_visits, 
  count(distinct case when event_type in ('view_listing') then ev.visit_id end) as view_listing_visits, 
  count(distinct case when event_type in ('shop_home') then ev.visit_id end) as shop_home_visits, 
  count(distinct case when event_type in ('cart_view') then ev.visit_id end) as cart_view_visits, 
  count(distinct case when event_type in ('home','homescreen') then ev.visit_id end) as home_visits, 
  count(distinct case when event_type in ('recommended') then ev.visit_id end) as recommended_visits, 
  count(distinct case when event_type in ('search') then ev.visit_id end) as search_visits, 
  count(distinct case when event_type in ('market') then ev.visit_id end) as market_visits, 

  sum(v.total_gms) as total_gms, 
  sum(case when event_type in ('view_listing') and ev.visit_id is not null then v.total_gms end) as view_listing_gms, 
  sum(case when event_type in ('shop_home') and ev.visit_id is not null then v.total_gms end) as shop_home_gms, 
  sum(case when event_type in ('cart_view') and ev.visit_id is not null then v.total_gms end) as cart_view_gms, 
  sum(case when event_type in ('home','homescreen') and ev.visit_id is not null then v.total_gms end) as home_gms, 
  sum(case when event_type in ('recommended') and ev.visit_id is not null then v.total_gms end) as recommended_gms, 
  sum(case when event_type in ('search') and ev.visit_id is not null then v.total_gms end) as search_gms, 
  sum(case when event_type in ('market') and ev.visit_id is not null then v.total_gms end) as market_gms, 
from 
  etsy-data-warehouse-prod.weblog.visits v
left join etsy-data-warehouse-prod.weblog.events ev using (visit_id)
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

--HOW MANY TIMES DO VISITS SEE THE SHOP HOME PAGE IN A VISIT
with visits_see_shop_home as (
select
  visit_id
from etsy-data-warehouse-prod.weblog.events
where _date >= current_date-30 AND event_type in ('shop_home')
)
, pageviews_per_visit as (
select
  visit_id
  , count(distinct case when event_type in ('shop_home') then sequence_number end) as shop_home_views
from visits_see_shop_home
inner join etsy-data-warehouse-prod.weblog.events using (visit_id)
where _date >= current_date-30 
group by all 
)
select avg(shop_home_views) from pageviews_per_visit
  --2.6976682591429628
