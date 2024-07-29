select
  v.platform
  , v.browser_platform
	, v.region
	, v.is_admin
	, v.top_channel
  , e.event_type
  , count(distinct e.visit_id) as unique_visits
  , count(e.visit_id) as total_visits 
  , count(distinct v.user_id) as unique_users
  , count (v.user_id) as total_users
  , count(distinct v.browser_id) as unique_browsers
  , count (v.browser_id) as total_browser_id
from
  etsy-data-warehouse-prod.weblog.visits v
inner join
  etsy-data-warehouse-prod.weblog.events e 
    using (visit_id, _date)
group by all 
where e.primary_page=1
