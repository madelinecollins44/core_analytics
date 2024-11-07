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
-- mapped_user_id	_date	next_visit_date	days_to_next_visit
-- 248	2024-10-11	2024-10-22	11
-- 248	2024-10-22	2024-11-04	13
-- 3586	2024-10-07	2024-10-29	22
-- 6283	2024-10-27	2024-11-05	9
-- 9706	2024-10-16	2024-10-27	11
-- 772	2024-10-16	2024-10-20	4
-- 772	2024-10-20	2024-10-22	2
-- 949	2024-11-02	2024-11-03	1
-- 1698	2024-10-27	2024-10-29	2
-- 1698	2024-10-29	2024-10-30	1
