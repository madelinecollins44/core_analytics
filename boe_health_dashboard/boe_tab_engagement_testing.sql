select 
_date
, sum(home_visits)
  from `etsy-data-warehouse-dev.madelinecollins.boe_tab_enage_testing`
group by all
-- 2024-08-18 : 8543207
-- 2024-08-19 : 9154338
-- 2024-08-20 : 8226505
-- 2024-08-21 : 8333932
-----ome visits
-- 2024-08-18 : 5903138
-- 2024-08-19 : 6310726
-- 2024-08-20 : 5749789
-- 2024-08-21 : 5811328

select _date, count(distinct visit_id) from `etsy-data-warehouse-prod.weblog.visits` where _date >= current_date-4 and platform in ('boe') group by all
-- 2024-08-18 : 8543207
-- 2024-08-19 : 9154338
-- 2024-08-20 : 8226505
-- 2024-08-21 : 8333932

select _date, count(distinct case when event_type in ('homescreen') then visit_id end) from `etsy-data-warehouse-prod.weblog.events` where _date >= current_date-4 group by all 
-- 2024-08-18 : 5906689
-- 2024-08-19 : 6314176
-- 2024-08-20 : 5752922
-- 2024-08-21 : 5814459

select 6310726/6314176
