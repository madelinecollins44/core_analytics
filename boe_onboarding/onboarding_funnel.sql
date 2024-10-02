--------------------------------------------------------------------------------------------------------
--get first visit info from each browser, and when the next three visits were 
--------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.boe_first_visits` AS (
select
  browser_id
  , user_id
  , platform
  , event_source
  , visit_id
  , registered
  , new_visitor
  , landing_event
  , exit_event
  , converted
  , case when user_id is not null then 1 else 0 end as signed_in
  , _date 
  , start_datetime
  , row_number() over (partition by browser_id order by start_datetime asc) as visit_rnk
  , lead(start_datetime, 1) over (partition by browser_id order by start_datetime asc) as next_visit
  , lead(start_datetime, 2) over (partition by browser_id order by start_datetime asc) as third_visit
from `etsy-data-warehouse-prod.weblog.visits`
where
    extract(year from _date) >= 2017
    and platform = 'boe'
);

--------------------------------------------------------------------------------------------------------
--breakdown actions in first visit
--------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.apallotto.boe_first_visit_actions` AS (
with first_visits as (
select
  * 
from `etsy-data-warehouse-dev.apallotto.boe_first_visits` 
where 
  visit_rnk =1
  and _date between current_date -30 and current_date-1
)
, events as (
select
  visit_id
  , max(case when event_type = "homescreen"  then 1 else 0 end) as homescreen
  , max(case when event_type in ("app_deals","deals_tab_delivered") then 1 else 0 end) as deals
  , max(case when event_type = "gift_mode_home" then 1 else 0 end) as gift_mode
  , max(case when event_type in ("you_tab_viewed","you_screen") then 1 else 0 end) as you
  , max(case when event_type IN ("favorites","favorites_and_lists","profile_favorite_listings_tab") then 1 else 0 end) as favorites_view
  , max(case when event_type = "cart_view" then 1 else 0 end) as cart
  , max(case when event_type = "cart_view" then 1 else 0 end) as listing_view

  , max(case when event_type = 'discover_theme_viewed' then 1 else 0 end) as discover_theme_viewed
  , max(case when event_type = 'login_view' then 1 else 0 end) as login_view
  , max(case when event_type = "sign_in_screen" then 1 else 0 end) as sign_in_screen
  , max(case when event_type = "third_party_signin" then 1 else 0 end) as social_sign_in
  , max(case when event_type = "magic_link_redeemed" then 1 else 0 end) as magic_sign_in
  , max(case when event_type = "onboarding_2019Q3" then 1 else 0 end) as register_sign_in
  , max(case when event_type = "continue_as_guest_tapped" then 1 else 0 end) as continue_as_guest_tap
  , max(case when event_type = "backend_log_in" then 1 else 0 end) as backend_log_in
  , max(case when event_type = "notification_registration_interstitial_viewed" then 1 else 0 end) as view_notification_opt_in
  , max(case when event_type = "notification_registration_interstitial_enable_tapped" then 1 else 0 end) as notification_opt_in_tap
  , max(case when event_type = "notification_registration_interstitial_dismiss_tapped" then 1 else 0 end) as notification_not_now_tap
  , max(case when event_type = "push_prompt_permission_granted" then 1 else 0 end) as push_allow_tap
  , max(case when event_type = "push_prompt_permission_denied" then 1 else 0 end) as push_dont_allow_tap
  , max(case when event_type = "favorites_onboarding_done_button_tapped" then 1 else 0 end) as fave_quiz_done_tap
  , max(case when event_type = "backend_favorite_item2" then 1 else 0 end) as favorited_item
from `etsy-data-warehouse-prod.weblog.events`
where 
  _date between current_date -30 and current_date-1
group by 
  1
)

select
  f.*
  , date(timestamp_seconds(m.join_date)) as join_date
  , e.discover_theme_viewed
  , e.login_view
  , e.sign_in_screen
  , e.magic_sign_in
  , e.social_sign_in
  , e.register_sign_in
  , e.continue_as_guest_tap
  , e.homescreen
  , e.deals
  , e.gift_mode
  , e.you
  , e.favorites_view
  , e.cart
  , e.listing_view  
  , e.view_notification_opt_in
  , e.notification_opt_in_tap
  , e.notification_not_now_tap
  , e.push_allow_tap
  , e.push_dont_allow_tap
  , e.fave_quiz_done_tap
  , e.favorited_item
  , e.backend_log_in


from first_visits f
left join events e 
  on f.visit_id = e.visit_id
left join `etsy-data-warehouse-prod.user_mart.mapped_user_profile` m
  on f.user_id = m.mapped_user_id
);

