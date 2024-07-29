--start with all visit data 
with agg_visit as (
select
  user_id
  , platform 
  , browser_platform
	, region  
  , is_admin_visit as admin
  , top_channel 
  , visit_id
  , _date
  , start_datetime
  , unix_seconds(timestamp (start_datetime)) as start_time)
  , row_number() over (partition by user_id order by unix_seconds(timestamp (start_datetime)) desc, visit_id desc) AS visit_order
from 
  etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-10
)
select * from agg_visit where user_id=266926560


, last_boe_visit as (
select 
    user_id
    , max(timestamp(split(visit_id, ".")[offset(1)])) as most_recent_visit
  -- , max(start_time) as most_recent_visit
from 
 (select user_id, max(start_time) from agg_visit_data where platform in ('boe')) a -- this gives me most recent day
group by all
)
select user_id, count(*) from last_boe_visit group by all order by 2 desc


-- , last_boe as (
-- select a.* 
-- from agg_visit_data a
-- inner join last_boe_visit b
--   on a.user_id=b.user_id
--   and a.start_time=b. most_recent_visit
-- )
-- select * from last_boe where user_id = 266926560
-- -- select user_id, count(*) from last_boe group by all order by 2 desc



-- -- , last_visit as (
-- -- select
-- --   user_id
-- --   , max(start_time) as most_recent_visit
-- -- from 
-- --   agg_visit_data
-- -- group by all
-- -- )
-- -- select
-- --   user_id
-- --   , platform 
-- --   , browser_platform
-- -- 	, region  
-- --   , admin
-- --   , top_channel 
-- -- from 
-- --   agg_visit_data
-- -- left join 
-- --   last_boe_visit
-- --     using ()
-- -- left join 
-- --   last_visit
