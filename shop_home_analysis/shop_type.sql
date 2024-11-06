
---------------------------------------------------------------------------
--overall traffic by shop type, landing traffic by shop type
---------------------------------------------------------------------------
-- find shop types 
-- create or replace table etsy-data-warehouse-dev.madelinecollins.visited_shop_ids as (
-- select 
-- 	(select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id,
-- 	(select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
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
  vs.raw_shop_shop_id,
  sb.seller_tier_new,
  sb.power_shop_status,
  sb.top_shop_status,
  sb.medium_shop_status,
  sb.small_shop_status
from 
  (select distinct raw_shop_shop_id from etsy-data-warehouse-dev.madelinecollins.visited_shop_ids) vs
left join 
  etsy-data-warehouse-prod.rollups.seller_basics sb 
    on vs.raw_shop_shop_id= cast(sb.shop_id as string)
group by all
)
--need to get shop_ids to visit level
, pageviews_per_shop as (
select
  raw_shop_shop_id,
  visit_id,
  count(sequence_number) as pageviews
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids
group by all
)
, add_in_gms as (
select
  a.raw_shop_shop_id,
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
, visit_level_metrics as (
select
  raw_shop_shop_id,
  count(distinct visit_id) as unique_visits,
  sum(pageviews) as pageviews,
  sum(total_gms) as total_gms,
from add_in_gms
group by all 
)
select
  seller_tier_new,
  count(distinct a.raw_shop_shop_id) as visited_shops,
  sum(unique_visits) as total_visits,
  sum(pageviews) as pageviews,
  sum(a.total_gms) as total_gms
from 
  visit_level_metrics a
left join 
  shop_tiers b using (raw_shop_shop_id)
group by all 


----------------------------------------------------------------
--by reporting channel
----------------------------------------------------------------
with shop_tiers as (
select
  vs.raw_shop_shop_id,
  sb.seller_tier_new,
  sb.power_shop_status,
  sb.top_shop_status,
  sb.medium_shop_status,
  sb.small_shop_status
from 
  (select distinct raw_shop_shop_id from etsy-data-warehouse-dev.madelinecollins.visited_shop_ids) vs
left join 
  etsy-data-warehouse-prod.rollups.seller_basics sb 
    on vs.raw_shop_shop_id= cast(sb.shop_id as string)
group by all
)
--need to get shop_ids to visit level
, pageviews_per_shop as (
select
  raw_shop_shop_id,
  visit_id,
  count(sequence_number) as pageviews
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids
group by all
)
, add_in_gms as (
select
case 
      when top_channel in ('direct') then 'Direct'
      when top_channel in ('dark') then 'Dark'
      when top_channel in ('internal') then 'Internal'
      when top_channel in ('seo') then 'SEO'
      when top_channel like 'social_%' then 'Non-Paid Social'
      when top_channel like 'email%' then 'Email'
      when top_channel like 'push_%' then 'Push'
      when top_channel in ('us_paid','intl_paid') then
        case
          when (second_channel like '%gpla' or second_channel like '%bing_plas') then 'PLA'
          when (second_channel like '%_ppc' or second_channel like 'admarketplace') then case
          when third_channel like '%_brand' then 'SEM - Brand' else 'SEM - Non-Brand'
          end
      when second_channel='affiliates' then 'Affiliates'
      when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
      when second_channel like '%native_display' then 'Display'
      when second_channel in ('us_video','intl_video') then 'Video' else 'Other Paid' end
      else 'Other Non-Paid' 
      end as reporting_channel,
  a.raw_shop_shop_id,
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
, visit_level_metrics as (
select
  reporting_channel,
  raw_shop_shop_id,
  count(distinct visit_id) as unique_visits,
  sum(pageviews) as pageviews,
  sum(total_gms) as total_gms,
from add_in_gms
group by all 
)
select
  reporting_channel,
  seller_tier_new,
  count(distinct a.raw_shop_shop_id) as visited_shops,
  sum(unique_visits) as total_visits,
  sum(pageviews) as pageviews,
  sum(a.total_gms) as total_gms
from 
  visit_level_metrics a
left join 
  shop_tiers b using (raw_shop_shop_id)
group by all 
