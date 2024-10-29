-----------------------------------------------------------------------------------------------------------------------------------------------
--Where do they go after shop home?  
----Prior screen, segment by visitors that purchase in-session vs. not, purchase something from the shop vs. not
---------------------------------------------------------------------------------------------------------------------------------------------
--overall traffic from shop home
with shop_home_visits as (
select
  visit_id,
  sequence_number,
  event_type,
  lag(event_type) over (partition by visit_id order by sequence_number) as previous_page
from 
  etsy-data-warehouse-prod.weblog.events
where
  _date >= current_date-30
  and page_view=1 
)
select
  previous_page,
  count(distinct visit_id) as visits
from 
  shop_home_visits
where 
  event_type in ('shop_home')
group by all 
order by 2 desc 


--how many listings views come from shop home?
select
  count(listing_id) as listing_views,
  count(case when referring_page_event in ('shop_home') then listing_id end) as shop_home_listing_views
from etsy-data-warehouse-prod.analytics.listing_views
where _date >= current_date-30
group by all  
