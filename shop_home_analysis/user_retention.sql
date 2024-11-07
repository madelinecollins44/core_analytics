--------------------------------------------------------------------------------------------------
--distribution of time since last visit for shop home visitors 
----users will be double counted if have different days between visits
--------------------------------------------------------------------------------------------------
--all shop home traffic 
with users_and_visits as (
select
  u.mapped_user_id,
  e._date,
  lag(e._date) over (partition by mapped_user_id order by _date) as last_visit_date,
  date_diff(e._date, lag(e._date) over (partition by mapped_user_id order by _date), day) as days_since_last_visit
from 
  etsy-data-warehouse-prod.weblog.events e
left join 
  `etsy-data-warehouse-prod.user_mart.user_mapping` u using (user_id)
where event_type in ('shop_home')
group by all 
)
select 
  days_since_last_visit,
  count(distinct mapped_user_id) as users
from users_and_visits
group by all 
order by 1 asc

---testing users with multiple visits
with users_and_visits as (
select
  u.mapped_user_id,
  e._date,
  lead(e._date) over (partition by mapped_user_id order by _date) as next_visit_date,
  date_diff(lead(e._date) over (partition by mapped_user_id order by _date), e._date, day) as days_to_next_visit
from 
  etsy-data-warehouse-prod.weblog.events e
left join 
  `etsy-data-warehouse-prod.user_mart.user_mapping` u using (user_id)
where event_type in ('shop_home')
group by all 
)
select * from users_and_visits where days_to_next_visit < 7 limit 5
-- mapped_user_id	_date	last_visit_date	days_since_last_visit
-- 2576	2024-10-26	2024-10-16	10
-- 2576	2024-11-04	2024-10-26	9
-- 8107	2024-10-19	2024-10-08	11
-- 9157	2024-10-17	2024-10-09	8
-- 13266	2024-10-31	2024-10-14	17
-- 2470	2024-10-15	2024-10-14	1
-- 2470	2024-10-16	2024-10-15	1
-- 4198	2024-10-10	2024-10-08	2
-- 4198	2024-10-15	2024-10-12	3
-- 4198	2024-10-16	2024-10-15	1
-- 2470	2024-10-24	2024-10-18	6

---testing users with null days between visits 
with users_and_visits as (
select
  u.mapped_user_id,
  e._date,
  lag(e._date) over (partition by mapped_user_id order by _date) as last_visit_id,
  date_diff(e._date, lag(e._date) over (partition by mapped_user_id order by _date), day) as days_since_last_visit
from 
  etsy-data-warehouse-prod.weblog.events e
left join 
  `etsy-data-warehouse-prod.user_mart.user_mapping` u using (user_id)
where 
  event_type in ('shop_home')
group by all 
)
select distinct _date from users_and_visits where days_since_last_visit is null limit 5
------users with null last_visit_dates
-- mapped_user_id	_date	last_visit_id	days_since_last_visit
-- 305	2024-10-18		
-- 1661	2024-10-21		
-- 2093	2024-11-02		
-- 2221	2024-10-08		
-- 3338	2024-10-21		
