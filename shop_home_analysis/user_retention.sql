--------------------------------------------------------------------------------------------------
--distribution of time since last visit for shop home visitors 
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

---testing
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
