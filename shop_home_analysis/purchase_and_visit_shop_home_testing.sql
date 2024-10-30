--testing query 
--find visit_ids, shop_ids, sequence number
with visited_shop_ids as (
select distinct
  visit_id
	, sequence_number
	, (select value from unnest(beacon.properties.key_value) where key = "shop_id") as shop_id
	, (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_shop_id
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` 
where 
  beacon.event_name in ('shop_home')
  and date(_partitiontime) >= current_date-5
)
-- visit_id	sequence_number	shop_id	shop_shop_id
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	404	36582243	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	407	36582243	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	449	36582243	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	522	36582243	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	772	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	821	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	866	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	910	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	954	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1036	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1049	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1084	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1128	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1154	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1194	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1244	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1401	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1534	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1588	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1638	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1682	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1738	42543487	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1922	40074783	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	1985	40074783	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	2035	40074783	
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	2076	40074783	

  , purchased_from_shops as (
select
  tv.visit_id, 
	t.seller_user_id,
	cast(sb.shop_id as string) as shop_id
	--shop_id here 
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
inner join
	etsy-data-warehouse-prod.transaction_mart.all_transactions t 
		using (transaction_id)
left join etsy-data-warehouse-prod.rollups.seller_basics sb
	on t.seller_user_id=sb.user_id
where tv.date >= current_date-5
)
-- visit_id	seller_user_id	shop_id
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	663258611	36582243
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	594136519	42543487
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	594136519	42543487
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	594136519	42543487
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1	594136519	42543487

  select
  a.visit_id,
  case when b.visit_id is not null then 1 else 0 end as also_visited
from purchased_from_shops a
-- thisd will switch to inner join 
left join visited_shop_ids b using (visit_id, shop_id)
order by 2 desc 
--just one particular visit_id to test 




--ALL VISIT IDS
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1
-- 3D102477EEDA4A918AF89886128C.1730227328478.1
-- jgY56_uNSP-X0SV_qBp8vg.1729985171591.1
-- 3D102477EEDA4A918AF89886128C.1730227328478.1
-- EA0158038D604EBEA8B3BE7DD081.1730050814052.1	
-- 9-z2rzZZRaKp7e3GL8ZRJQ.1730214572374.1
-- y1PmoEIYRAqzrolWmAomvg.1730222075814.1
-- KGMicFvbSLm_wDoFToLouA.1729910338746.1
-- ALDDJWvuSQOLtO7HPWbKOQ.1729843767463.2
-- LBKthuiZRhypyKjOC_HB4Q.1729865980462.6
-- wmyqDCacQDCYk1-aCA7Owg.1730189713070.1
-- uZ3IQfonTyqoYhooeF4oxg.1730133484100.1
-- uZ3IQfonTyqoYhooeF4oxg.1730133484100.1
-- uZ3IQfonTyqoYhooeF4oxg.1730133484100.1
-- uZ3IQfonTyqoYhooeF4oxg.1730133484100.1
-- 8934F3308E3C4C4BA6B26CBDE480.1730173696975.1
