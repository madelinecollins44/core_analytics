------------------------------------------------------------------------------------------------------------------------------------------------------------
--instances sign_in_screen without a full_gate property
-----sign in on splash screen: full_gate = true
-----sign in on homescreen: full_gate = false
-----sign in event without a full_gate property = older versions of app? 
------------------------------------------------------------------------------------------------------------------------------------------------------------
--within the first visit for each browser in the last 30 days, what is the distribution of full_gate properties? 
with first_browser_visits as (
  select 
    browser_id, 
    visit_id 
from etsy-data-warehouse-dev.madelinecollins.boe_first_visits 
  where visit_rnk = 1 
  and _date >= current_date-30
  and event_source in ('ios')
)
select      
  event_name,
  full_gate,
  count(distinct browser_id) as browsers,
  count(distinct visit_id) as visits
from 
  first_browser_visits
inner join 
  etsy-data-warehouse-dev.madelinecollins.app_onboarding_events 
  using(visit_id)
where 
  event_name in ('sign_in_screen')
group by all
-- event_name	full_gate	browsers	visits
-- sign_in_screen	false	60844	60844
-- sign_in_screen	true	1593959	1593959
-- sign_in_screen		409446	409446


------------------------------------------------------------------------------------------------------------------------------------------------------------
--how often does login_view fire before the homescreen view? this is to see if login_view can be a reliable event we use to track sign in web views
------------------------------------------------------------------------------------------------------------------------------------------------------------
with first_browser_visits as (
  select 
    browser_id, 
    visit_id 
from etsy-data-warehouse-dev.madelinecollins.boe_first_visits 
  where visit_rnk = 1 
  and _date >= current_date-30
  and event_source in ('ios') -- these onboarding events are only for ios 
)
, agg as (
select  
  visit_id,
  min(case when event_name  in ('login_view') then sequence_number end) as first_login_view_sequence_number,
  min(case when event_name = "homescreen_complementary" and first_view in ("true") then sequence_number end) as first_homescreen_sequence_number
from 
  first_browser_visits v 
left join 
  (select * from etsy-data-warehouse-dev.madelinecollins.app_onboarding_events where event_name in ('login_view','homescreen_complementary')) e
      using (visit_id)
group by all
)
select 
  count(distinct case when first_login_view_sequence_number < first_homescreen_sequence_number then visit_id end) as login_before_home,
  count(distinct case when first_login_view_sequence_number is not null then visit_id end) visits_with_login_view,
  count(distinct case when first_homescreen_sequence_number is not null then visit_id end) visits_with_homescreen,
  count(distinct visit_id) as unique_visits 
from agg
group by all 
-- login_before_home	visits_with_login_view	visits_with_homescreen	unique_visits
-- 441213	753760	1496535	2043862

------------------------------------------------------------------------------------------------------------------------------------------------------------
--how often does register_view from the register from web screen for? 
------------------------------------------------------------------------------------------------------------------------------------------------------------
 with first_browser_visits as (
  select 
    browser_id, 
    visit_id 
from etsy-data-warehouse-dev.madelinecollins.boe_first_visits 
  where visit_rnk = 1 
  and _date >= current_date-30
  and event_source in ('ios')
)
 select
    count(distinct visit_id) as visits,
    count(distinct beacon.browser_id) as browsers,
  from etsy-visit-pipe-prod.canonical.visit_id_beacons 
  where 
    date(_partitiontime) >= current_date-30
    and beacon.event_name in ('register_view')
 
  -------testing
  with first_browser_visits as (
  select 
    browser_id, 
    visit_id 
from etsy-data-warehouse-dev.madelinecollins.boe_first_visits 
  where visit_rnk = 1 
  and _date >= current_date-30
  and event_source in ('ios')
)
  
select * from first_browser_visits where visit_id in ('776C33ECB47249BE9F206475F7E0.1730779804960.1')
 select
    a.visit_id,
    max(case when b.visit_id is null then 1 else 0 end) as first_visit
  from etsy-visit-pipe-prod.canonical.visit_id_beacons  a
  left join first_browser_visits b using (visit_id)
  where 
    date(_partitiontime) >= current_date-30
    and beacon.event_name in ('register_view')
  group by all 
  order by 2 desc
  ---visits that arent in first visit on boe but did get a register_view event
--776C33ECB47249BE9F206475F7E0.1730779804960.1
--F39674EF54034800B50F925580F7.1730569856004.1
--5C20B4B8F2EF454EB655F6F7902C

select * from etsy-visit-pipe-prod.canonical.visit_id_beacons where visit_id in ('776C33ECB47249BE9F206475F7E0.1730779804960.1') and 
    date(_partitiontime) >= current_date-30 order by sequence_number asc

select * from etsy-data-warehouse-prod.weblog.visits where visit_id in ('776C33ECB47249BE9F206475F7E0.1730779804960.1') and _date >= current_date-30
