
---------------------------------------------------------------------------
--overall traffic by shop type, landing traffic by shop type
---------------------------------------------------------------------------
--find shop types 
-- create or replace table etsy-data-warehouse-dev.madelinecollins.visited_shop_ids as (
-- select 
-- 	(select value from unnest(beacon.properties.key_value) where key = "shop_id") as raw_shop_id,
-- 	(select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as raw_shop_shop_id,
--   coalesce((select value from unnest(beacon.properties.key_value) where key = "shop_id") , (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id")) as shop_id,
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

--CHECK 
  -- select visit_id, count(shop_id) from add_in_gms group by all order by 2 asc
--WbtCZohzj0J0UQyL7edwRCocHcLW.1728986489452.2, 1888
--ucXKOuI4maSrH2MO6Pz3vWJoQLkl.1729506369635.2, 1883
-- CF88E1C77CBC436482F77ED5BE67.1730136154040.2, 0

--   , visit_level_metrics as (
-- select
--   shop_id,
--   count(distinct visit_id) as unique_visits,
--   sum(pageviews) as pageviews,
--   sum(total_gms) as total_gms,
-- from add_in_gms
-- where visit_id in ('ucXKOuI4maSrH2MO6Pz3vWJoQLkl.1729506369635.2,')
-- group by all 
)
