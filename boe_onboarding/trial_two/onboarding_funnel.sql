------------------------------------------------
--find first BOE visit for each browser
------------------------------------------------
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
    and pages_seen > 1 -- filters out visits that didnt see a primary page, ensures more purposeful visits  
);

------------------------------------------------
--pull all events associated with onboarding
------------------------------------------------
 create or replace table etsy-data-warehouse-dev.madelinecollins.app_onboarding_events as (
 select
    beacon.event_name  as event_name,
    (select value from unnest(beacon.properties.key_value) where key = "first_view") as first_view,
    (select value from unnest(beacon.properties.key_value) where key = "full_gate") as full_gate,
    visit_id,
    sequence_number
  from etsy-visit-pipe-prod.canonical.visit_id_beacons 
  where date(_partitiontime) >= current_date-30
      and (beacon.event_name in (
      ---- LOG IN SPLASH SCREEN
      'sign_in_screen', -- PRIMARY EVENT FOR THIS SCREEN. distinction made by fullgate property, fullgate = true 
      'continue_as_guest_tapped', -- continue as guest 
      'BOE_social_sign_in_tapped',-- log in w socials BUT fires across screens 
      'BOE_etsy_sign_in_tapped', -- log in w email  BUT fires across screens 
      ----REGISTRAION WEB VIEW
      'join_submit',
      'BOE_email_sign_in_webview_cancelled',
      'register_view'
      ----SIGN IN WEB VIEW
      'signin_submit', -- PRIMARY EVENT FOR THIS SCREEN.
      'magic_link_click',
      'magic_link_send',
      'magic_link_redeemed',
      'magic_link_error',
      'forgot_password',
      'forgot_password_view',
      'forgot_password_email_sent',
      'reset_password',
      'keep_me_signed_in_checked',
      'keep_me_signed_in_unchecked',
      'login',
      'BOE_email_sign_in_webview_cancelled',
      'login_view' -- would be great to use this but unsure if this fires in beacons table
      ----NOTIFICATIONS OPT IN 1 
      'notification_registration_interstitial_enable_tapped',
      'notification_registration_interstitial_dismiss_tapped',
      'update_setting',
      'notification_registration_interstitial_viewed', -- PRIMARY EVENT FOR THIS SCREEN.
      ----NOTIFICATIONS OPT IN 2
      'push_prompt_permission_granted', -- PRIMARY EVENT FOR THIS SCREEN.
      'push_prompt_permission_denied', -- PRIMARY EVENT FOR THIS SCREEN.
      ----APP TRACKING TRANSPARENCY PROMPT
      'app_tracking_transparency_system_prompt_denied_tapped', -- PRIMARY EVENT FOR THIS SCREEN.
      'app_tracking_transparency_system_prompt_authorized_tapped', -- PRIMARY EVENT FOR THIS SCREEN.
      ----SIGN IN FROM HOMESCREEN
      'login_view',
      'BOE_social_sign_in_tapped',
      'sign_in_screen', -- PRIMARY EVENT FOR THIS SCREEN. distinction made by fullgate property, fullgate = false 
      ----FAVORITES QUIZ 
      'favorites_onboarding_skip_button_tapped',
      'favorites_onboarding_done_button_tapped',
      'favorites_onboarding_viewed')-- PRIMARY EVENT FOR THIS SCREEN.
      ----------the following sections are removed due to the fact that these are not onboarding requirements. 
      -- ----INITIAL CONTENT (SKIPPED QUIZ)
      -- -- 'homescreen',
      -- 'homescreen_recommended_Categories_seen',
      -- 'boe_homescreen_popular_-clusters_seen',
      -- 'scrolled_past_boe_homescreen_recs_placholder_-module_4',
      -- 'recommendations_module_seen'
      -- ----INITIAL CONTENT (COMPLETED QUIZ)
      -- 'homescreen_top_banner_marketing_Merch3_event_viewed',
      -- 'boe_homescreen_home_hub_viewed',
      -- 'scrolled_past_boe_homescreen_home_hub',
      -- 'scrolled_past_homescreen_our_picks',
      -- 'recommended'
      -- ----HOMESCREEN WITH ACTIVITY 
      -- 'boe_evergreen_interests_recs_module_delivered',
      -- 'boe_homescreen_home_hub_delivered',
      -- 'boe_active_mission_recs_module_delivered',
      -- 'boe_homescreen_tab_delivered')
    or       
  ----SIGNED OUT HOMESCREEN
      (beacon.event_name = "homescreen_complementary"
      and (select value from unnest(beacon.properties.key_value) where key = "first_view") in ("true")))
    --   'open_url',
    --   'sign_in_screen' --fullgate property = false 
    -- -- this is for homescreen post sign in page )
  group by all 
);

----------------------------------------------------------------------------------------------------------------------------------------
--test to see how often login view (event for web sign in) fires before homescreen to see if login view is reliable event for that screen
----------------------------------------------------------------------------------------------------------------------------------------
with agg as (
select  
  visit_id,
  case when beacon.event_name  in ('login_view') then sequence_number end as login_view_sequence_number,
  case when (beacon.event_name = "homescreen_complementary" and (select value from unnest(beacon.properties.key_value) where key = "first_view") in ("true")) then sequence_number end as homescreen_sequence_number
from etsy-visit-pipe-prod.canonical.visit_id_beacons 
where date(_partitiontime) >= current_date-30
)
select 
  count(distinct case when login_view_sequence_number < homescreen_sequence_number then visit_id end) as login_before_home,
  count(distinct case when login_view_sequence_number is not null then visit_id end) visits_with_login_view,
  count(distinct case when homescreen_sequence_number is not null then visit_id end) visits_with_homescreen,
  count(distinct visit_id) as unique_visits 
from agg
group by all 