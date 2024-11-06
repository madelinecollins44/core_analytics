--------------------------------------------------------------------------------------------------
--how often are all users revisiting the shop_home page? 
--------------------------------------------------------------------------------------------------
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
select 
  days_to_next_visit,
  count(distinct mapped_user_id) as users
from users_and_visits
group by all 
order by 1 desc

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
select * from users_and_visits limit 5
mapped_user_id	    _date	          next_visit_date	    days_to_next_visit
813	              2024-11-04		
1616	            2024-10-14		
1700	            2024-10-09	      2024-10-10	              1
1700	            2024-10-10	      2024-10-14	              4
1700	            2024-10-14	      2024-10-15	              1
248	              2024-10-11  	    2024-10-22	              11
248		             2024-10-22	  	  2024-11-04	              13
3586		           2024-10-07  	  2024-10-29	              22
6283		           2024-10-27	  	 2024-11-05		              9
9706		           2024-10-16	  	  2024-10-27		              11
