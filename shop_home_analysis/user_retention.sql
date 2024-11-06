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
