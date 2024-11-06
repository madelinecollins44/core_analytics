--Where do they go after shop home?
----Next screen, segment by visitors that purchase in-session vs. not, purchase something from the shop vs. not

---------------------------------------------------------------------------------------------------------------------------------------------
--overall traffic
---------------------------------------------------------------------------------------------------------------------------------------------
with shop_home_visits as (
select
  visit_id,
  sequence_number,
  event_type,
  lead(event_type) over (partition by visit_id order by sequence_number) as next_page
from 
  etsy-data-warehouse-prod.weblog.events
where
  _date >= current_date-30
  and page_view=1 
)
select
  next_page,
  count(visit_id) as pageviews,
  count(distinct visit_id) as unique_visits
from 
  shop_home_visits
where 
  event_type in ('shop_home')
group by all 
order by 2 desc 

---------------------------------------------------------------------------------------------------------------------------------------------
--visits that convert
---------------------------------------------------------------------------------------------------------------------------------------------
with shop_home_visits as (
select
  visit_id,
  sequence_number,
  event_type,
  lead(event_type) over (partition by visit_id order by sequence_number) as next_page
from 
  etsy-data-warehouse-prod.weblog.events
where
  _date >= current_date-30
  and page_view=1 
)
select
  next_page,
  count(visit_id) as pageviews,
  count(distinct visit_id) as visits
from 
  shop_home_visits shv
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  event_type in ('shop_home')
  and v.converted > 0 
  and v._date >= current_date-30
group by all 
order by 2 desc 

---------------------------------------------------------------------------------------------------------------------------------------------
--testing next page
----------------------------------------------------------------------------------------------------
with shop_home_visits as (
select
  visit_id,
  sequence_number,
  event_type,
  lead(event_type) over (partition by visit_id order by sequence_number) as next_page
from 
  etsy-data-warehouse-prod.weblog.events
where
  _date >= current_date-30
  and page_view=1 
)
select * from shop_home_visits where visit_id in ('Aa4HjNXASTuQllHw7jRVX-EkvKkO.1729604034909.1') order by sequence_number
-- visit_id
-- Aa4HjNXASTuQllHw7jRVX-EkvKkO.1729604034909.1
-- F184540D4E4A42D0A176B238ABAC.1729628143025.1
-- 51drlvo7imZaRcI58ygBdFQtRmcd.1729570304896.1
-- 7WSEtOH34ZvOdF_gDUPsCyRRxH8F.1728535064727.1
-- jyd0Y47wvFyvj_8ctnY9U-verWT5.1728588738168.1
-- M-LIN9qP_C6w04izWU3LrPOxs6ie.1728543553623.1
-- 25E11C3FD9B84FC58C517BBFBC79.1727730868908.1
-- hOGR4JIl62zJbscBtHaOSLuIBhIY.1729388067464.1
-- Xh8nsVkne_kh4A3UQLApn3_9wefs.1727658268573.1
-- 79DD923D1254458D8D18267ED56F.1729627814584.2
-- P7nG9BfMXyrv_XcOs2MOuurrFe7C.1728546549273.1
-- 9_5oocT7NqqNo0PbS6azfKcbLk3Z.1727679108695.1
-- j8Uo7zdHDGMKcfVJTBeRRXx0SDEZ.1729961056582.1


-- select
--   next_page,
--   count(distinct visit_id) as visits
-- from 
--   shop_home_visits
-- where 
--   event_type in ('shop_home')
-- group by all 
-- order by 2 desc 

---------------------------------------------------------------------------------------------------------------------------------------------
--purchase rate of listings from shop home
---------------------------------------------------------------------------------------------------------------------------------------------
select
  referring_page_event,
  count(listing_id) as listing_views,
  count(case when purchased_after_view > 0 then listing_id end) as purchased_listings
from etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
group by all 

--how many listings views come from shop home?
select
case when referring_page_event in ('browselistings','search') then 'browselistings / search'
  else referring_page_event
  end as referring_page_event,,
  count(listing_id) as listing_views,
  count(case when purchased_after_view > 0 then listing_id end) as purchased_listings,
  count(case when purchased_after_view > 0 then listing_id end)/count(listing_id) as purchase_rate
from etsy-data-warehouse-prod.analytics.listing_views
where 
  _date >= current_date-30
group by all 
order by 4 desc

---------------------------------------------------------------------------------------------------------------------------------------------
--entire page jounrey
---------------------------------------------------------------------------------------------------------------------------------------------
	with shop_home_visits as (
select
  visit_id,
  sequence_number,
  event_type,
  lead(event_type) over (partition by visit_id order by sequence_number) as next_page,
  lag(event_type) over (partition by visit_id order by sequence_number) as previous_page
from 
  etsy-data-warehouse-prod.weblog.events
where
  _date >= current_date-30
  and page_view=1 
)
select
  next_page,
  previous_page,
  count(distinct visit_id) as visits
from 
  shop_home_visits shv
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  event_type in ('shop_home')
  and v._date >= current_date-30
group by all 
order by 2 desc 

---------------------------------------------------------------------------------------------------------------------------------------------
--next page of visits that have purchased from the shop itself
---------------------------------------------------------------------------------------------------------------------------------------------
--get visit info of when a visit see a shop_home page
with purchased_from_shops as (
select
  tv.visit_id, 
	t.seller_user_id,
	cast(sb.shop_id as string) as shop_id,
  count(transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
inner join
	etsy-data-warehouse-prod.transaction_mart.all_transactions t 
		using (transaction_id)
left join etsy-data-warehouse-prod.rollups.seller_basics sb
	on t.seller_user_id=sb.user_id
where tv.date >= current_date-30
group by all 
)
, visits_to_home_and_purchase as (
select
 b.visit_id,
 b.sequence_number, -- need this so can join to next page
 b.shop_id,
 case when a.transactions is not null then 1 else 0 end as transactions
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids b 
left join 
  purchased_from_shops a using (visit_id, shop_id)
group by all
)
,  next_page as (
select
  visit_id,
  sequence_number,
  event_type,
  lead(event_type) over (partition by visit_id order by sequence_number) as next_page
from 
  etsy-data-warehouse-prod.weblog.events
where
  _date >= current_date-30
  and page_view=1 
)
--look at the next_page for anyone that views the shop_home page + has purchased from that shop in visit
select 
	np.next_page,
	np.event_type,
	count(vh.visit_id) as pageviews,
	count(distinct vh.visit_id) as unique_visits
from visits_to_home_and_purchase vh
inner join next_page np using (visit_id, sequence_number)
group by all
---------------------------------------------------------------------------------------------------------------------------------------------
--testing of purchase  visit
---------------------------------------------------------------------------------------------------------------------------------------------
--get visit info of when a visit see a shop_home page
with purchased_from_shops as (
select
  tv.visit_id, 
	t.seller_user_id,
	cast(sb.shop_id as string) as shop_id,
  count(transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
inner join
	etsy-data-warehouse-prod.transaction_mart.all_transactions t 
		using (transaction_id)
left join etsy-data-warehouse-prod.rollups.seller_basics sb
	on t.seller_user_id=sb.user_id
where tv.date >= current_date-30
group by all 
)
-- visit_id	seller_user_id	shop_id	transactions
-- FzzfQD6oSVWA0akkgMsckg.1729683003991.3	857550370	48131995	2 ---> DIDNT VISIT
-- 6sTdZyEr2cR8seFr2R71rBelF9no.1730266340471.1	46996441	35942283	1
-- pIRpDtpczJyPhy0uCyA7hld3QSbs.1729958036513.3	148655008	17516993	2
-- 4m45rQuJ_fsD_lzb2tJN4JU5OCXd.1730403701095.3	589707121	33652197	7
-- NjCKL1_ERbS7UZjQsR9aIg.1730388118286.1	556065882	36880361	1

--find visits that have purchased from store, and when they visited the store within that visit 
, visits_to_home_and_purchase as (
select
 b.visit_id,
 b.sequence_number, -- need this so can join to next page
 b.shop_id,
 case when a.transactions is not null then 1 else 0 end
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids b 
left join 
  purchased_from_shops a using (visit_id, shop_id)
group by all
)
select * from purchased_from_shops where visit_id in ('Y9m3qVDsH1JusgXUtHayvEC6Q26l.1729682073017.2','0WHikVi8PBzqMS24QHK_XE6nPN6j.1728406555941.2	530	29669151','sNEDNrnE6MtmjsgpeXy7RrwzveZ2.1730082341929.1','6h4mQRv2EazdlhEyati0-WUExjml.1729483820853.2','A-NLqm4OiwWC19qkzp0R8YjjL-3L.1730770007674.1') and shop_id in ('17970214','29669151','7957168','40557450','51953130')
--find visits with purchased 
, visits_to_home_and_purchase as (
select
 b.visit_id,
 b.sequence_number, -- need this so can join to next page
 b.shop_id,
 case when a.transactions is not null then 1 else 0 end as transactions
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids b 
left join 
  purchased_from_shops a using (visit_id, shop_id)
group by all
)
select * from visits_to_home_and_purchase where transactions > 0 limit 5  
-----------the following are all NOT in the purchased table
-- visit_id	sequence_number	shop_id	f0_
-- Y9m3qVDsH1JusgXUtHayvEC6Q26l.1729682073017.2	349	17970214	0
-- 0WHikVi8PBzqMS24QHK_XE6nPN6j.1728406555941.2	530	29669151	0
-- sNEDNrnE6MtmjsgpeXy7RrwzveZ2.1730082341929.1	0	6803186	0
-- 6h4mQRv2EazdlhEyati0-WUExjml.1729483820853.2	0	7957168	0
-- A-NLqm4OiwWC19qkzp0R8YjjL-3L.1730770007674.1	79	51953130	0

-----------the following are all in the purchased table
-- NjCKL1_ERbS7UZjQsR9aIg.1730388118286.1	556065882	36880361	1
-- 63F0564E903242B2A0B02D3EB72D.1728410365885.1	469	53551553	1
-- _p7YpLub21-cd8YbnckDJWN2XnmV.1729194431120.1	250	46670827	1
-- _YhuBqXLjqMhOGB8dQ3KgwmEMxnO.1730741363304.2	255	40557450	1
-- C3B5436F04734D698E701A257B41.1730313226888.1	120	41991232	1
-- D525975BBD664DD08DEF67C49DB1.1728665975144.1	837	50444114	1

	
---testing to make sure purchased are in that table
with purchased_from_shops as (
select
  tv.visit_id, 
	t.seller_user_id,
	cast(sb.shop_id as string) as shop_id,
  count(transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
inner join
	etsy-data-warehouse-prod.transaction_mart.all_transactions t 
		using (transaction_id)
left join etsy-data-warehouse-prod.rollups.seller_basics sb
	on t.seller_user_id=sb.user_id
where tv.date >= current_date-30
group by all 
)
select * from purchased_from_shops where visit_id in ('NjCKL1_ERbS7UZjQsR9aIg.1730388118286.1','63F0564E903242B2A0B02D3EB72D.1728410365885.1','_p7YpLub21-cd8YbnckDJWN2XnmV.1729194431120.1','_YhuBqXLjqMhOGB8dQ3KgwmEMxnO.1730741363304.2','C3B5436F04734D698E701A257B41.1730313226888.1','D525975BBD664DD08DEF67C49DB1.1728665975144.1') and shop_id in ('36880361','53551553','46670827','40557450','41991232','50444114')
-- NjCKL1_ERbS7UZjQsR9aIg.1730388118286.1	556065882	36880361	1
-- 63F0564E903242B2A0B02D3EB72D.1728410365885.1	469	53551553	1
-- _p7YpLub21-cd8YbnckDJWN2XnmV.1729194431120.1	250	46670827	1
-- _YhuBqXLjqMhOGB8dQ3KgwmEMxnO.1730741363304.2	255	40557450	1
-- C3B5436F04734D698E701A257B41.1730313226888.1	120	41991232	1
-- D525975BBD664DD08DEF67C49DB1.1728665975144.1	837	50444114	1
