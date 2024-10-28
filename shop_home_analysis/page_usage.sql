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

---------------------------------------------------------------------------------------------------------------------------------------------
--Which types of buyers go to shop home? 
----Buyer segment, visit channel, platform, past 7d visits, X listing views in session, engaged visits, signed in vs signed out, left reviews
---------------------------------------------------------------------------------------------------------------------------------------------
----REPORTING CHANNEL 
-- case 
--       when top_channel in ('direct') then 'Direct'
--       when top_channel in ('dark') then 'Dark'
--       when top_channel in ('internal') then 'Internal'
--       when top_channel in ('seo') then 'SEO'
--       when top_channel like 'social_%' then 'Non-Paid Social'
--       when top_channel like 'email%' then 'Email'
--       when top_channel like 'push_%' then 'Push'
--       when top_channel in ('us_paid','intl_paid') then
--         case
--           when (second_channel like '%gpla' or second_channel like '%bing_plas') then 'PLA'
--           when (second_channel like '%_ppc' or second_channel like 'admarketplace') then case
--           when third_channel like '%_brand' then 'SEM - Brand' else 'SEM - Non-Brand'
--           end
--       when second_channel='affiliates' then 'Affiliates'
--       when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
--       when second_channel like '%native_display' then 'Display'
--       when second_channel in ('us_video','intl_video') then 'Video' else 'Other Paid' end
--       else 'Other Non-Paid' 
--       end as reporting_channel

----SIGNED IN VS SIGNED OUT 
  -- case when v.user_id is null or v.user_id = 0 then "signed out"
  -- else "signed in"
  -- end as status
 
----PLATFORM   
select 
--platform
  , count(distinct visit_id) as visits 
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where event_type in ('shop_home')
and v._date >= current_date-30
group by all

----ENGAGED VISITS 
select
  count(distinct visit_id) as total_visits,
 count(distinct case 
      when timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0
    then v.visit_id end) as engaged_visits,
  count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and event_type in ('shop_home')
    then v.visit_id end) as shop_home_engaged_visits   
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where v._date >= current_date-30
group by all

----REVISIT WITHIN 7 DAYS
------users that see the shop_home page, and then visit again within 7 days 
with next_visit as (
select
  mapped_user_id,
  v._date,
  v.start_datetime,
  visit_id,
  lead(v._date) over (partition by mapped_user_id order by v.start_datetime asc) as next_visit_date
from 
  `etsy-data-warehouse-prod.weblog.visits` v  
left join  
  etsy-data-warehouse-prod.user_mart.user_mapping um using (user_id)
where 
  v._date >= current_date-30
group by all
)
select 
  count(distinct visit_id) as visits,
  count(distinct mapped_user_id) as users,
  count(distinct case when event_type in ('shop_home') then visit_id end) as shop_home_visits,
  count(distinct case when event_type in ('shop_home') then mapped_user_id end) as shop_home_users,
from 
  next_visit v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and date_diff(v.next_visit_date,v._date, day) <=7
group by all


----BUYER SEGMENT
  -- begin
-- create or replace temp table buyer_segments as (select * from etsy-data-warehouse-prod.rollups.buyer_segmentation_vw where as_of_date >= current_date-30);
-- end 
--------etsy-bigquery-adhoc-prod._script5dba7009b5483b12d9ab6aa377f829e47d355146.buyer_segments

with all_shop_home_visits as (
select distinct
  user_id
  , visit_id
from etsy-data-warehouse-prod.weblog.events
where
  _date >= current_date-30
  and event_type in ('shop_home')
)
select 
  buyer_segment
  , count(distinct visit_id) as total_visits
  , count(distinct um.mapped_user_id) as users 
from 
  all_shop_home_visits v
left join 
  etsy-data-warehouse-prod.user_mart.user_mapping um  
    on v.user_id=um.user_id
left join 
  etsy-bigquery-adhoc-prod._script5dba7009b5483b12d9ab6aa377f829e47d355146.buyer_segments bs
    on um.mapped_user_id=bs.mapped_user_id
group by all

---listing views 
with listing_views as (
select 
  visit_id
  , count(listing_id) as listings_viewed
from etsy-data-warehouse-prod.analytics.listing_views
where _date >= current_date-30
group by all
)
select 
  count(distinct visit_id) as total_visits_with_listing_views,
  count(distinct case when listings_viewed >=1 and event_type in ('shop_home') then visit_id end) as _1_plus_listings_viewed,
  count(distinct case when listings_viewed >=5 and event_type in ('shop_home') then visit_id end) as _5_plus_listings_viewed,
  count(distinct case when listings_viewed >=10 and event_type in ('shop_home') then visit_id end) as _10_plus_listings_viewed,
  count(distinct case when listings_viewed >=20 and event_type in ('shop_home') then visit_id end) as _20_plus_listings_viewed,
from 
  listing_views v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  e._date >= current_date-30
group by all

---------------------------------------------------------------------------------------------------------------------------------------------
--How do they get to shop home?  
----Prior screen, segment by visitors that purchase in-session vs. not, purchase something from the shop vs. not
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
