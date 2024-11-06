
------------------------------------------------------------------------------------------------------------------------------------------------------
--test this table to see how many seller_user_ids, shop_ids are not in seller_basics table
------------------------------------------------------------------------------------------------------------------------------------------------------
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

------testing seller user ids
select
  count(distinct vsi.seller_user_id) as beacons_properties,
  count(distinct sb.user_id) as seller_basics,
  count(distinct case when sb.user_id is null then vsi.seller_user_id end) as beacons_without_match,
  count(distinct sb.user_id) / count(distinct vsi.seller_user_id) as share_with_match, 
  count(distinct case when sb.user_id is null then vsi.seller_user_id end)/ count(distinct vsi.seller_user_id) as share_without_match
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids vsi 
left join 
  etsy-data-warehouse-prod.rollups.seller_basics sb
    on vsi.seller_user_id=cast(sb.user_id as string)
group by all
-- beacons_properties	    seller_basics	    beacons_without_match	    share_with_match	      share_without_match
-- 7519878	              6388219	          1131659	                  0.849510989407009	       0.150489010592991

------testing shop ids
select
  count(distinct vsi.shop_id) as beacons_properties,
  count(distinct sb.shop_id) as seller_basics,
  count(distinct case when sb.shop_id is null then vsi.shop_id end) as beacons_without_match,
  count(distinct sb.shop_id) / count(distinct vsi.shop_id) as share_with_match, 
  count(distinct case when sb.shop_id is null then vsi.shop_id end)/ count(distinct vsi.shop_id) as share_without_match
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids vsi 
left join 
  etsy-data-warehouse-prod.rollups.seller_basics sb
    on vsi.shop_id=cast(sb.shop_id as string)
group by all
-- beacons_properties	      seller_basics	      beacons_without_match	      share_with_match	      share_without_match
-- 7069431	                6359212	            710219	                    0.89953661051363254	     0.10046338948636743

------testing to see quality of shop home event
select
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
-- missing_shop_ids	    missing_seller_user_ids	    has_both	    missing_both	    total_shop_home_visits	    share_missing_shop_ids	    share_missing_seller_user_ids	      share_has_both	      share_missing_both
-- 45967984	            116739391	                   241324433	   5128256	        409160064	                    0.11234719134270152	        0.28531472465504354	            0.58980446586302226	      0.01253361813923267
  
