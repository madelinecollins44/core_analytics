------------------------------------------------------------------------------------------------------------------------------------------------------------
--instances sign_in_screen without a full_gate property
-----sign in on splash screen: full_gate = true
-----sign in on homescreen: full_gate = false
-----sign in event without a full_gate property = older versions of app? 
------------------------------------------------------------------------------------------------------------------------------------------------------------
select      
  event_name,
  first_view,
  full_gate,
  count(distinct visit_id)
from etsy-data-warehouse-dev.madelinecollins.app_onboarding_events 
where event_name in ('sign_in_screen')
group by all
-- event_name	first_view	full_gate	f0_
-- sign_in_screen			1791147
-- sign_in_screen		true	4191728
-- sign_in_screen		false	185287


------------------------------------------------------------------------------------------------------------------------------------------------------------
--how often does login_view fire before the homescreen view? this is to see if login_view can be a reliable event we use to track sign in web views
------------------------------------------------------------------------------------------------------------------------------------------------------------
with first_browser_visits as (
  select 
    browser_id, 
    new_visitor,
    visit_id 
from etsy-data-warehouse-dev.madelinecollins.boe_first_visits 
  where visit_rnk = 1 
  and _date >= current_date-30
  and event_source in ('ios') -- these onboarding events are only for ios 
)
, agg as (
select  
  visit_id,
  case when event_name  in ('login_view') then sequence_number end as login_view_sequence_number,
  case when event_name = "homescreen_complementary" and first_view in ("true") then sequence_number end as homescreen_sequence_number
from first_browser_visits v 
left join etsy-data-warehouse-dev.madelinecollins.app_onboarding_events  e
      using (visit_id)
)
select 
  count(distinct case when login_view_sequence_number < homescreen_sequence_number then visit_id end) as login_before_home,
  count(distinct case when login_view_sequence_number is not null then visit_id end) visits_with_login_view,
  count(distinct case when homescreen_sequence_number is not null then visit_id end) visits_with_homescreen,
  count(distinct visit_id) as unique_visits 
from agg
group by all 
-- login_before_home	visits_with_login_view	visits_with_homescreen	unique_visits
-- 0	                      745459	                1482734              	2027134
