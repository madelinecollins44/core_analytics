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
