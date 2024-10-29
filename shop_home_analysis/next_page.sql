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
  count(distinct visit_id) as visits
from 
  shop_home_visits
where 
  event_type in ('shop_home')
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
