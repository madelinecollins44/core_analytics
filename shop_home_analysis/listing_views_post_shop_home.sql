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
, all_visits as (
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
  fv.first_sequence_number < lv.referring_page_event_sequence_number -- listing views happen after shop home 
  and lv._date >= current_date-30
group by all 
)
select
  count(distinct visit_id) as visits_with_lv,
  sum(total_listing_views) as total_lv,
  sum(shop_home_listing_view) as shop_home_lv,
  sum(other_listing_view) as other_lv,
  avg(shop_home_listing_view) as avg_shop_home_lv_per_visit,
  avg(other_listing_view) as avg_other_lv_per_visit,
from all_visits
group by all
---testing 
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

select * from etsy-data-warehouse-prod.analytics.listing_views lv where _date >= current_date - 30 and visit_id in ('Cs2RoVsoE9Df_lgsW4FSG_B7I_-G.1730609731023.1')
-- visit_id	first_sequence_number	total_listing_views	shop_home_listing_view	other_listing_view
-- cIwEMdCWQWKJgTed1fxofg.1730377778653.1	537	1	1	0
-- kXsteSAYN1gTy2admKFOo8GdjT6E.1730332959685.1	367	8	0	0
-- D52116DB1B5247B2BC7264388BF6.1730597377261.1	14	6	0	6
-- Cs2RoVsoE9Df_lgsW4FSG_B7I_-G.1730609731023.1	285	3	0	3
-- am6CvPD-TfKppC7Xlcwaeg.1730649407956.2	30	17	4	13

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
select * from etsy-data-warehouse-prod.weblog.events where visit_id in ('Cs2RoVsoE9Df_lgsW4FSG_B7I_-G.1730609731023.1') and sequence_number = 285
