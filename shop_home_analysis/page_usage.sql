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

--engaged types within shop home traffic 
select
  count(distinct visit_id) as total_visits,
  count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 then v.visit_id end) as long_visits,
  count(distinct case when v.cart_adds> 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 then v.visit_id end) as favoriting_visit,
  count(distinct case when v.converted > 0 then v.visit_id end) as converted_visits, 
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and event_type in ('shop_home')
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

---users that revisit shop home within 7 days 
with next_visit as (
select
  mapped_user_id,
  v._date,
  v.start_datetime,
  v.visit_id,
  lead(v._date) over (partition by mapped_user_id order by v.start_datetime asc) as next_visit_date
from 
  `etsy-data-warehouse-prod.weblog.visits` v  
left join  
  etsy-data-warehouse-prod.user_mart.user_mapping um using (user_id)
inner join 
  etsy-data-warehouse-prod.weblog.events e 
    on v.visit_id=e.visit_id
where 
  v._date >= current_date-30
  and event_type in ('shop_home')
group by all
)
select 
  count(distinct visit_id) as visits,
  count(distinct mapped_user_id) as users,
from 
  next_visit v
where 
  date_diff(v.next_visit_date,v._date, day) <=7
group by all



----BUYER SEGMENT
  -- begin
-- create or replace temp table buyer_segments as (select * from etsy-data-warehouse-prod.rollups.buyer_segmentation_vw where as_of_date >= current_date-30);
-- end 
-------- etsy-bigquery-adhoc-prod._script096f9ed76a29597dfe9d74159ae108364d865800.buyer_segments

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
   etsy-bigquery-adhoc-prod._script096f9ed76a29597dfe9d74159ae108364d865800.buyer_segments bs
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
  count(distinct case when listings_viewed >=2 and event_type in ('shop_home') then visit_id end) as _2_plus_listings_viewed,
  count(distinct case when listings_viewed >=5 and event_type in ('shop_home') then visit_id end) as _5_plus_listings_viewed,
  count(distinct case when listings_viewed >=10 and event_type in ('shop_home') then visit_id end) as _10_plus_listings_viewed,
  -- count(distinct case when listings_viewed >=20 and event_type in ('shop_home') then visit_id end) as _20_plus_listings_viewed,
from 
  listing_views v
inner join 
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  e._date >= current_date-30
group by all


---------------------------------------------------------------------------------------------------------------------------------------------
--What are the most used parts of the page? 
----Scroll depth, clicks, etc
----Segment by visitors that purchase in-session vs. not, purchase something from the shop vs. not
---------------------------------------------------------------------------------------------------------------------------------------------

select
  event_type,
  count(distinct visit_id) as visits
from etsy-data-warehouse-prod.weblog.events e  
inner join etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  v._date >= current_date-30
  and event_type like ('shop_home%')
  and v.platform in ('mobile_web','desktop')
-- and v.platform in ('boe')
group by all


-----THE FOLLOWING ARE EVENTS I SAW FIRE IN EVENTHORIZON BUT DID NOT SEE IN EVENTHUB
--count visits with these events 
-- with page_actions as (
-- select distinct
--   visit_id
-- 	, sequence_number
--   , beacon.event_name as event_name
-- 	, (select value from unnest(beacon.properties.key_value) where key = "shop_id") as shop_id
-- from 
--   `etsy-visit-pipe-prod.canonical.visit_id_beacons` 
-- where 
--   date(_partitiontime) >= current_date-30
--  and beacon.event_name in (
--     'shop_home', -- main page
--     'shop_home_about_section_seen', -- scroll to about section
--     'shop_home_reviews_pagination', -- sort reviews
--     'shop_home_reviews_section_top_seen', -- scroll to reviews section
--     'shop_home_policies_section_seen', -- scroll to store policies
--     'shop_home_listings_section_seen',-- scroll to top listings section
--     'shop_home_listing_grid_seen', -- scroll to listing grid
--     'shop_home_branding_section_seen',-- see branding at top of page
--     'shop_home_announcement_section_seen',-- see branding at top of page
--     'footer_seen', -- first footer seen, subscribe to etsy, bottom of shop home page
--     'registry_ingress_footer_seen', -- very bottom of page, etsy download app cta, shop/ sell/ about/ help options
--     'shop_about_new_video_play', -- clicked on video
--     'shop_home_branded_carousel_arrow_click', -- clicked through carousel at top of page 
--     'shop_home_items_pagination', -- click on next page of listings
--     'shop_home_branded_carousel_pagination_click',-- click through carousel
--     'favorite_shop',-- favorite the shop
--     'shop_home_reviews_jump_link_hover', -- click on reviews, brings directly to bottom
--     'chat_dialog_open', -- starts chat with seller
--     'shop_home_section_select', -- click on listing filter in the grid
--     'neu_favorite_click', -- favorite item in listing grid
--     'shop_home_dropdown_engagement'-- filter listing grid
--     )
-- )
-- select 
--   count(distinct case when event_name in ('shop_home') then visit_id end) as shop_home,
--   count(distinct case when event_name in ('shop_home_about_section_seen') then visit_id end) as shop_home_about_section_seen,
--   count(distinct case when event_name in ('shop_home_reviews_pagination') then visit_id end) as shop_home_reviews_pagination,
--   count(distinct case when event_name in ('shop_home_reviews_section_top_seen') then visit_id end) as shop_home_reviews_section_top_seen,
--   count(distinct case when event_name in ('shop_home_policies_section_seen') then visit_id end) as shop_home_policies_section_seen,
--   count(distinct case when event_name in ('shop_home_listings_section_seen') then visit_id end) as shop_home_listings_section_seen,
--   count(distinct case when event_name in ('shop_home_listing_grid_seen') then visit_id end) as shop_home_listing_grid_seen,
--   count(distinct case when event_name in ('shop_home_branding_section_seen') then visit_id end) as shop_home_branding_section_seen,
--   count(distinct case when event_name in ('shop_home_announcement_section_seen') then visit_id end) as shop_home_announcement_section_seen,
--   count(distinct case when event_name in ('footer_seen') then visit_id end) as footer_seen,
--   count(distinct case when event_name in ('registry_ingress_footer_seen') then visit_id end) as registry_ingress_footer_seen,
--   count(distinct case when event_name in ('shop_about_new_video_play') then visit_id end) as shop_about_new_video_play,
--   count(distinct case when event_name in ('shop_home_branded_carousel_arrow_click') then visit_id end) as shop_home_branded_carousel_arrow_click,
--   count(distinct case when event_name in ('shop_home_items_pagination') then visit_id end) as shop_home_items_pagination,
--   count(distinct case when event_name in ('shop_home_branded_carousel_pagination_click') then visit_id end) as shop_home_branded_carousel_pagination_click,
--   count(distinct case when event_name in ('favorite_shop') then visit_id end) as favorite_shop,
--   count(distinct case when event_name in ('shop_home_reviews_jump_link_hover') then visit_id end) as shop_home_reviews_jump_link_hover,
--   count(distinct case when event_name in ('chat_dialog_open') then visit_id end) as chat_dialog_open,
--   count(distinct case when event_name in ('shop_home_section_select') then visit_id end) as shop_home_section_select,
--   count(distinct case when event_name in ('neu_favorite_click') then visit_id end) as neu_favorite_click,
--   count(distinct case when event_name in ('shop_home_dropdown_engagement') then visit_id end) as shop_home_dropdown_engagement,
-- from page_actions


---------------------------------------------------------------------------
--deeper dive on landings
---------------------------------------------------------------------------
--what share of landings are shop_home
  select
  count(distinct case when landing_event in ('shop_home') then visit_id end) as shop_home_landings,
  count(distinct visit_id) as total_visits,
  count(distinct case when landing_event in ('shop_home') then visit_id end)/ count(distinct visit_id) as share_of_visits
from etsy-data-warehouse-prod.weblog.visits  
where _date >= current_date-30
-- shop_home_landings	    total_visits	      share_of_visits
-- 78165051	              1081360060	        0.072284018886364265

--how many visits to shop_home are landings
select
  count(distinct case when v.landing_event in ('shop_home') then v.visit_id end) as shop_home_landings,
  count(distinct v.visit_id) as all_visits,
  count(distinct case when landing_event in ('shop_home') then visit_id end) / count(distinct v.visit_id) as share
from 
  etsy-data-warehouse-prod.weblog.visits v
inner join  
  etsy-data-warehouse-prod.weblog.events e using (visit_id)
where 
  v._date >= current_date-30
  and e.event_type in ('shop_home')


---------------------------------------------------------------------------
--overall traffic by shop type, landing traffic by shop type
---------------------------------------------------------------------------
--overall traffic 
--find all visits to shop
-- create or replace table etsy-data-warehouse-dev.madelinecollins.visited_shop_ids as (
-- select 
-- 	(select value from unnest(beacon.properties.key_value) where key = "shop_id") as shop_id,
--   visit_id,
--   sequence_number
-- from 
--   `etsy-visit-pipe-prod.canonical.visit_id_beacons` 
-- where 
--   beacon.event_name in ('shop_home')
--   and date(_partitiontime) >= current_date-30
-- );

with shop_tiers as (
select
  vs.shop_id,
  sb.seller_tier,
  sb.sws_status,
  sb.high_potential_seller_status,
  sb.top_seller_status,
  sb.power_seller_status
from 
  (select distinct shop_id from etsy-data-warehouse-dev.madelinecollins.visited_shop_ids) vs
left join 
  etsy-data-warehouse-prod.rollups.seller_basics sb 
    on vs.shop_id= cast(sb.shop_id as string)
group by all
--need to get shop_ids to visit level
with pageviews_per_shop as (
select
  shop_id,
  visit_id,
  count(sequence_number) as pageviews
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids
group by all
)
, add_in_gms as (
select
  a.shop_id,
  a.visit_id,
  a.pageviews,
  sum(b.total_gms) as total_gms
from 
  pageviews_per_shop a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  _date >= current_date-30
group by all 
)
select visit_id, count(shop_id) from add_in_gms group by all order by 2 desc
--WbtCZohzj0J0UQyL7edwRCocHcLW.1728986489452.2, 1888
--ucXKOuI4maSrH2MO6Pz3vWJoQLkl.1729506369635.2, 1883

-- , visit_level_metrics as (
select
  shop_id,
  count(distinct visit_id) as unique_visits,
  sum(pageviews) as pageviews,
  sum(total_gms) as total_gms,
from add_in_gms
group by all 
-- )

