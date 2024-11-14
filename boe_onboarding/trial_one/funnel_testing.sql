---------------------------------------------------------------------------------------------------
--consistency across # browsers with first visit in last 30 days
---------------------------------------------------------------------------------------------------
with first_browser_visits as (
  select 
    browser_id, 
    event_source, 
    new_visitor,
    visit_id 
from etsy-data-warehouse-dev.madelinecollins.boe_first_visits 
  where visit_rnk = 1 
  and _date >= current_date-30
)
-- select count(distinct browser_id) from first_browser_visits
  ---39227364
 , event_counts as (
  select
    e.event_type as event_name,
    v.visit_id,
    v.browser_id,
    v.event_source,
    v.new_visitor,
    case when e.visit_id is null then 1 else 0 end as no_onboarding_events 
from first_browser_visits v 
 left join `etsy-data-warehouse-prod.weblog.events` e
      using (visit_id)
  where event_type in (
      --log in spalsh screen 
      'sign_in_screen',
      'continue_as_guest_tapped',
      'login_view',
      'BOE_social_sign_in_tapped',
      'BOE_etsy_sign_in_tapped',

      --Registration Web View
      'join_submit',
      'BOE_email_sign_in_webview_cancelled',

      --Sign In Web View
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
      'BOE_email_sign_in_webview_cancelled',

      --Sign In from Homescreen
      'login_view',
      'BOE_social_sign_in_tapped',

      --Notifications Opt In pt 1
      'notification_registration_interstitial_enable_tapped',
      'notification_registration_interstitial_dismiss_tapped',
      'update_setting',
      'notification_registration_interstitial_viewed',

      --Notifications Opt In pt 2
      'push_prompt_permission_granted',
      'push_prompt_permission_denied',
      'app_tracking_transparency_system_prompt_denied_tapped',
      'app_tracking_transparency_system_prompt_authorized_tapped',
      'scrolled_past_onboarding_favorites-q4-2019',

      --Favorites Quiz
      'onboarding_favorites-q4-2019_viewed',
      'onboarding_faves_tapped_skip',
      'favorites_onboarding_done_button_tapped',

      --homescreen/ initial content
      'homescreen',
      'boe_homescreen_tab_delivered',
      'recommendations_module_seen'
    )
  group by all 
)
-- select count(distinct browser_id) from event_counts
  --37853177

-- select 39227364-37853177
--1374187

-- select 1374187/39227364
--3.5% of browsers that visit for the first time dont have any of these events ?????
---------------------------------------------------------------------------------------------------
--checking on browsers that view boe for the first time but dont see any onboarding events>? 
---------------------------------------------------------------------------------------------------

  -- create or replace table etsy-data-warehouse-dev.madelinecollins.boe_onboarding_funnel_events as (
with first_browser_visits as (
  select 
    browser_id, 
    event_source, 
    new_visitor,
    visit_id 
from etsy-data-warehouse-dev.madelinecollins.boe_first_visits 
  where visit_rnk = 1 
  and _date >= current_date-30
)
-- select count(distinct browser_id) from first_browser_visits
--   ---39227364
 , event_counts as (
  select
    e.event_type as event_name,
    v.visit_id,
    v.browser_id,
    v.event_source,
    v.new_visitor,
    case when e.visit_id is null then 1 else 0 end as no_onboarding_events 
from first_browser_visits v 
 left join `etsy-data-warehouse-prod.weblog.events` e
      using (visit_id)
  where event_type in (
      --log in spalsh screen 
      'sign_in_screen',
      'continue_as_guest_tapped',
      'login_view',
      'BOE_social_sign_in_tapped',
      'BOE_etsy_sign_in_tapped',

      --Registration Web View
      'join_submit',
      'BOE_email_sign_in_webview_cancelled',

      --Sign In Web View
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
      'BOE_email_sign_in_webview_cancelled',

      --Sign In from Homescreen
      'login_view',
      'BOE_social_sign_in_tapped',

      --Notifications Opt In pt 1
      'notification_registration_interstitial_enable_tapped',
      'notification_registration_interstitial_dismiss_tapped',
      'update_setting',
      'notification_registration_interstitial_viewed',

      --Notifications Opt In pt 2
      'push_prompt_permission_granted',
      'push_prompt_permission_denied',
      'app_tracking_transparency_system_prompt_denied_tapped',
      'app_tracking_transparency_system_prompt_authorized_tapped',
      'scrolled_past_onboarding_favorites-q4-2019',

      --Favorites Quiz
      'onboarding_favorites-q4-2019_viewed',
      'onboarding_faves_tapped_skip',
      'favorites_onboarding_done_button_tapped',

      --homescreen/ initial content
      'homescreen',
      'boe_homescreen_tab_delivered',
      'recommendations_module_seen'
    )
  group by all 
)
select * from event_counts where no_onboarding_events = 1
---------------------------------------------------------------------------------------------------
--testing first visit table to make sure it actually pulls first visit from each browser
---------------------------------------------------------------------------------------------------
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
    extract(year from _date) >= 2017 -- since 2017, when was each browsers first visit
    and platform = 'boe'
qualify row_number() over (partition by browser_id order by start_datetime asc) =1
);



---------------------------------------------------------------------------------------------------
--looking at table on browser level, making sure its doing what we want
---------------------------------------------------------------------------------------------------
-- select * from etsy-data-warehouse-dev.madelinecollins.boe_onboarding_funnel_events where browser_id="76A49588B1B74630B1C0259E552D" order by screen asc
select browser_id, event_name, count(*) from etsy-data-warehouse-dev.madelinecollins.boe_onboarding_funnel_events group by all
----all unique
