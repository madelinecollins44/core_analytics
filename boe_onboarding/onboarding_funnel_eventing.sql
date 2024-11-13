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
      'sign_in_screen', -- this also captures sign in from homescreen. distinction made by fullgate property, fullgate = true 
      'continue_as_guest_tapped', -- continue as guest 
      'BOE_social_sign_in_tapped',-- log in w socials 
      'BOE_etsy_sign_in_tapped', -- log in w email 
      ----REGISTRAION WEB VIEW
      'join_submit',
      'BOE_email_sign_in_webview_cancelled',
      'register_view'
      ----SIGN IN WEB VIEW
      'signin_submit',
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
      'login_view'
      ----NOTIFICATIONS OPT IN 1 
      'notification_registration_interstitial_enable_tapped',
      'notification_registration_interstitial_dismiss_tapped',
      'update_setting',
      'notification_registration_interstitial_viewed',
      ----NOTIFICATIONS OPT IN 2
      'push_prompt_permission_granted',
      'push_prompt_permission_denied',
      ----APP TRACKING TRANSPARENCY PROMPT
      'app_tracking_transparency_system_prompt_denied_tapped',
      'app_tracking_transparency_system_prompt_authorized_tapped',
      ----SIGN IN FROM HOMESCREEN
      'login_view',
      'BOE_social_sign_in_tapped',
      ----FAVORITES QUIZ 
      'onboarding_favorites-q4-2019_viewed',
      'onboarding_faves_tapped_skip',
      'favorites_onboarding_done_button_tapped',
      'scrolled_past_onboarding_favorites-q4-2019',
      ----INITIAL CONTENT (SKIPPED QUIZ)
      -- 'homescreen',
      'homescreen_recommended_Categories_seen',
      'boe_homescreen_popular_-clusters_seen',
      'scrolled_past_boe_homescreen_recs_placholder_-module_4',
      'recommendations_module_seen'
      ----INITIAL CONTENT (SKIPPED QUIZ)
      'homescreen_top_banner_marketing_Merch3_event_viewed',
      'boe_homescreen_home_hub_viewed',
      'scrolled_past_boe_homescreen_home_hub',
      'scrolled_past_homescreen_our_picks',
      'recommended'
      ----HOMESCREEN WITH ACTIVITY 
      'boe_evergreen_interests_recs_module_delivered',
      'boe_homescreen_home_hub_delivered',
      'boe_active_mission_recs_module_delivered',
      'boe_homescreen_tab_delivered')
    or       
  ----SIGNED OUT HOMESCREEN
      (beacon.event_name = "homescreen_complementary"
      and (select value from unnest(beacon.properties.key_value) where key = "first_view") in ("true")))
    --   'open_url',
    --   'sign_in_screen' --fullgate property = false 
    -- -- this is for homescreen post sign in page )
  group by all 
);
