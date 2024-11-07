--------------------------------------------------------------------------------------------------
--distribution of time since last visit for shop home visitors for general traffic + landing on shop home
----users will be double counted if have different days between visits
--------------------------------------------------------------------------------------------------
with users_and_visits as (
select
  u.mapped_user_id,
  e._date,
  lag(e._date) over (partition by mapped_user_id order by e._date) as last_visit_id,
  date_diff(e._date, lag(e._date) over (partition by mapped_user_id order by e._date), day) as days_since_last_visit
from 
  (select * from etsy-data-warehouse-prod.weblog.events where event_type in ('shop_home')) e
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
left join 
  `etsy-data-warehouse-prod.user_mart.user_mapping` u 
    on e.user_id=u.user_id -- use user_id when 
where 
  event_type in ('shop_home')
  -- and v.landing_event in ('shop_home')
  and v._date >= current_date-30
group by all 
)
select 
  days_since_last_visit, 
  count(distinct mapped_user_id) as unique_users,
  count(mapped_user_id) as users,
from 
  users_and_visits 
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

---testing users with null days between visits : these users have no previous shop home visit
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

--------------------------------------------------------------------------------------------------
--distribution of time since last visit for shop home visitors that donot land on the page
---wanted to do it like this so dont exclude those that land on the page
--------------------------------------------------------------------------------------------------
  --pull out all sequence numbers of landing events
with landing_events as (
select
  user_id,
  visit_id,
  _date,
  sequence_number,
  event_type,
from 
  etsy-data-warehouse-prod.weblog.events
where page_view =1 
qualify row_number () over (partition by user_id order by sequence_number) = 1-- pulling first primary event
)
--look at shop home views that happened AFTER the landing event
, visits_shop_home_post_landing as (
select
  e.user_id,
  e.visit_id,
  e._date,
from landing_events le
inner join etsy-data-warehouse-prod.weblog.events e
    on le.visit_id=e.visit_id
    and le.user_id=e.user_id
    and le.sequence_number < e.sequence_number -- everything after the landing event
where e.event_type in ('shop_home')
)
, users_and_visits as (
select
  u.mapped_user_id,
  e._date,
  lag(e._date) over (partition by mapped_user_id order by e._date) as last_visit_date,
  date_diff(e._date, lag(e._date) over (partition by mapped_user_id order by e._date), day) as days_since_last_visit
from 
  visits_shop_home_post_landing e
left join 
  `etsy-data-warehouse-prod.user_mart.user_mapping` u using (user_id)
group by all 
)
select 
  days_since_last_visit, 
  count(distinct mapped_user_id) as unique_users,
  count(mapped_user_id) as users,
from 
  users_and_visits 
group by all 
order by 1 asc

--testing 
  -- user_id	visit_id	_date
-- 453664481	FLtnkjPaR5W-JNdq3qgyWQ.1729401404109.1	2024-10-20
-- 445545202	5mS9cvxdHWPGmR0nPZ_w__XYv9nl.1730634493297.1	2024-11-03
-- 998691047	-XsmD-GS51je9hWRQ2lRrZDX2Srr.1730407737936.1	2024-10-31
-- 998691047	-XsmD-GS51je9hWRQ2lRrZDX2Srr.1730407737936.1	2024-10-31
-- 640630156	inzqj70BSytRSsXe-bCSQJoXBdc0.1729920420280.1	2024-10-26
select * from etsy-data-warehouse-prod.weblog.events where visit_id in ('FLtnkjPaR5W-JNdq3qgyWQ.1729401404109.1') and page_view=1 order by sequence_number asc
select landing_event from etsy-data-warehouse-prod.weblog.visits where visit_id in ('-XsmD-GS51je9hWRQ2lRrZDX2Srr.1730407737936.1') and _date >= current_date-30
