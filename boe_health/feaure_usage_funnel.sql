with small_events as (
select
event_type,
visit_id,
_date
from `etsy-data-warehouse-prod.weblog.events`
where event_type in ('homescreen','deals_tab_delivered','app_deals','gift_mode_home','favorites_and_lists','cart_view','browselistings')
order by visit_id, _date
)
-- , count_events as (
select
  e.visit_id
  ,s.buyer_segment
  , max(case when e.event_type in ('homescreen') then 1 else 0 end) as saw_home
  , max(case when e.event_type in ('deals_tab_delivered','app_deals') then 1 else 0 end) as saw_deals
  , max(case when e.event_type in ('gift_mode_home') then 1 else 0 end) as saw_gift_mode_home
  , max(case when e.event_type in ('favorites_and_lists') then 1 else 0 end) as saw_favorites_and_lists
  , max(case when e.event_type in ('cart_view') then 1 else 0 end) as saw_cart_view
  , max(case when e.event_type in ('browselistings') then 1 else 0 end) as saw_browselistings
from small_events e
left join etsy-data-warehouse-prod.rollups.visits_w_segments s using (visit_id)
group by all
)
select
  v._date
  , v.browser_platform
  , v.region
  , count(distinct v.visit_id) as total_visits
  , count(distinct case when saw_home > 0 then e.visit_id end) as home_visits
  , count(distinct case when saw_deals > 0 then e.visit_id end) as deals_visits
  , count(distinct case when saw_gift_mode_home > 0 then e.visit_id end) as gift_mode_home_visits
  , count(distinct case when saw_favorites_and_lists > 0 then e.visit_id end) as favorites_visits
  , count(distinct case when saw_cart_view > 0 then e.visit_id end) as cart_view_visits
  , count(distinct case when saw_browselistings > 0 then e.visit_id end) as search_visits
  , count(distinct case when sum(saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings) > 1 then e.visit_id end) as one_plus_tabs
  , count(distinct case when sum(saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings) > 2 then e.visit_id end) as two_plus_tabs
  , count(distinct case when sum(saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings) > 3 then e.visit_id end) as three_plus_tabs
  , count(distinct case when sum(saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings) > 4 then e.visit_id end) as four_plus_tabs        
  , count(distinct case when sum(saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings) > 5 then e.visit_id end) as five_plus_tabs
    , count(distinct case when sum(saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings) > 6 then e.visit_id end) as six_plus_tabs
from 
  `etsy-data-warehouse-prod.weblog.recent_visits` v
left join 
  small_events e using(visit_id, _date)
where
  v._date >= current_date-30
  and v.platform = 'boe'
  and v.event_source = 'ios'
  and v.app_name = 'ios-EtsyInc'
;
