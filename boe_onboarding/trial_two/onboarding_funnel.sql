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
--get browser counts for each event in the onboarding process 
----------------------------------------------------------------------------------------------------------------------------------------
with first_browser_visits as (
  select 
    browser_id, 
    visit_id 
from etsy-data-warehouse-dev.madelinecollins.boe_first_visits 
  where visit_rnk = 1 
  and _date >= current_date-30
  and event_source in ('ios')
)
--of those browsers, how many completed each onboarding event 
 , event_counts as (
  select
    event_name,
    first_view,
    full_gate,
    v.visit_id,
    v.browser_id,
from first_browser_visits v 
left join etsy-data-warehouse-dev.madelinecollins.app_onboarding_events  e
      using (visit_id)
  group by all
)
SELECT
    event_name,
    CASE 
     WHEN (event_name IN ('sign_in_screen') and full_gate in ('true')) -- PRIMARY EVENT FOR THIS SCREEN. distinction made by fullgate property, fullgate = true 
      or (event_name  IN (
      'continue_as_guest_tapped', -- continue as guest 
      'BOE_social_sign_in_tapped',-- log in w socials BUT fires across screens 
      'BOE_etsy_sign_in_tapped') -- log in w email  BUT fires across screens 
      ) THEN '1 - Log In Splash Screen'
      
      WHEN event_name  IN (
      'join_submit',
      'BOE_email_sign_in_webview_cancelled',
      'register_view'
      ) THEN '2 - Registration Web View'
     
      WHEN event_name  IN (
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
      'BOE_email_sign_in_webview_cancelled'
      ) THEN '3 - Sign In Web View'
      
      WHEN (event_name IN ('homescreen_complementary') and first_view = "true")
        THEN '4 - First Homescreen View'
      
      WHEN event_name  IN (
      'notification_registration_interstitial_enable_tapped',
      'notification_registration_interstitial_dismiss_tapped',
      'update_setting',
      'notification_registration_interstitial_viewed' -- PRIMARY EVENT FOR THIS SCREEN.
      ) THEN '5 - Notifications Opt In pt 1'
      
      WHEN event_name  IN (
      'push_prompt_permission_granted', -- PRIMARY EVENT FOR THIS SCREEN.
      'push_prompt_permission_denied' -- PRIMARY EVENT FOR THIS SCREEN.
      ) THEN '6 - Notifications Opt In pt 2'
      
      WHEN event_name  IN (
      'app_tracking_transparency_system_prompt_denied_tapped', -- PRIMARY EVENT FOR THIS SCREEN.
      'app_tracking_transparency_system_prompt_authorized_tapped' -- PRIMARY EVENT FOR THIS SCREEN.
      ) THEN '7 - App Tracking Transparency Prompt'
      
      WHEN event_name  IN (
      'favorites_onboarding_skip_button_tapped',
      'favorites_onboarding_done_button_tapped',
      'favorites_onboarding_viewed'-- PRIMARY EVENT FOR THIS SCREEN.
      ) THEN '8 - Favorites Quiz'

     WHEN (event_name IN ('sign_in_screen') and full_gate in ('false'))
       THEN '9 - Sign In from Homescreen'
    
      ELSE NULL
    END as screen,
    count(distinct browser_id) as browsers
FROM 
  event_counts
group by all 
ORDER BY screen, event_name
