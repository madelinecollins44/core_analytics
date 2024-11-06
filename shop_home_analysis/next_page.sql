---------------------------------------------------------------------------------------------------------------------------------------------
--Where do they go after shop home?
----Next screen, segment by visitors that purchase in-session vs. not, purchase something from the shop vs. not
---------------------------------------------------------------------------------------------------------------------------------------------
--overall traffic to shop home
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

--visits that convert 
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

--next page testing 
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

---purchase rate of listings viewed from shop_home
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

--entire page jounrey
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


--next page of visits that have purchased from the shop itself
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
--find visits that have purchased from store, and when they visited the store within that visit 
, visits_to_home_and_purchase as (
select
 b.visit_id,
 b.sequence_number, -- need this so can join to next page
 b.raw_shop_shop_id,
 a.transactions
from etsy-data-warehouse-dev.madelinecollins.visited_shop_ids b 
inner join purchased_from_shops a 
	on b.visit_id=a.visit_id
	and b.raw_shop_shop_id=a.shop_id
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

