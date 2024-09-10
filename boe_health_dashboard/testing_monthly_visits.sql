---code used to test monthly visits per user section in dashboard 
with agg as (
select 
  visit_id
  , _date
  , coalesce(cast(user_id as string), browser_id) as unique_id
  , platform
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-365)
select
  date_trunc(_date, month) as month
  , platform
  , count(distinct visit_id)/ count(distinct unique_id) as monthly_visits_per_user
from agg
group by all 

--platform in last 30 days 
