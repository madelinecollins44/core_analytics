------------------------------------------------------------------------------------------------------
--distribution of recent listing views for shop home visitors (and split by internal SH view vs. landers)
------------------------------------------------------------------------------------------------------
with first_shop_home_view as (
select
  visit_id,
  min(sequence_number) as first_sequence_number
from 
  etsy-data-warehouse-prod.weblog.events
where 
  event_type in ('shop_home')
group by all 
)
select
  lv.visit_id,
  count(listing_id) as total_listing_views,
  count(case when referring_page_event in ('shop_home') then listing_id end) as shop_home_listing_view,
  count(case when referring_page_event not in ('shop_home') then listing_id end) as other_listing_view
from 
  etsy-data-warehouse-prod.analytics.listing_views lv
inner join 
  first_shop_home_view fv using (visit_id)
where 
  fv.first_sequence_number < lv.sequence_number -- listing views happen after shop home 
  and lv._date >= current_date-30
group by all 
limit 5
