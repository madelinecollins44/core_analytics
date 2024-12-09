-- -- find shop types 
-- create or replace table etsy-data-warehouse-dev.madelinecollins.visited_shop_ids as (
-- select 
--   platform,
-- 	(select value from unnest(beacon.properties.key_value) where key = "shop_id") as seller_user_id,
-- 	(select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_id,
--   visit_id,
--   sequence_number
-- from 
--   `etsy-visit-pipe-prod.canonical.visit_id_beacons` 
-- inner join 
--   etsy-data-warehouse-prod.weblog.visits using (visit_id)
-- where 
--   beacon.event_name in ('shop_home')
--   and date(_partitiontime) >= current_date-30
--   and _date >= current_date-30
-- group by all 
-- );

-- select visit_id, sequence_number, count(*) from etsy-data-warehouse-dev.madelinecollins.visited_shop_ids group by all order by 3 desc limit 5
-----ALL EVENTS ARE UNIQUE

select
  -- platform,
  count(case when seller_user_id is not null and shop_id is null then visit_id end) as missing_shop_ids,
  count(case when shop_id is not null and seller_user_id is null then visit_id end) as missing_seller_user_ids,
  count(case when seller_user_id is not null and shop_id is not null then visit_id end) as has_both,
  count(case when seller_user_id is null and shop_id is null then visit_id end) as missing_both,
  count(visit_id) as total_shop_home_visits,
--shares 
  count(case when seller_user_id is not null and shop_id is null then visit_id end)/ count(visit_id) as share_missing_shop_ids,
  count(case when shop_id is not null and seller_user_id is null then visit_id end)/ count(visit_id) as share_missing_seller_user_ids,
  count(case when seller_user_id is not null and shop_id is not null then visit_id end)/count(visit_id) as share_has_both,
  count(case when seller_user_id is null and shop_id is null then visit_id end)/ count(visit_id) as share_missing_both
from etsy-data-warehouse-dev.madelinecollins.visited_shop_ids vsi 
group by all
