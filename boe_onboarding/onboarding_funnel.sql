--------------------------------------------------------------------------------------------------------
--get first visit info from each browser, and when the next three visits were 
------when looking at each browsers first visit, need to filter to visit_rnk = 1 to look at first visit
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
--onboarding event funnel
--------------------------------------------------------------------------------------------------------

with first_browser_visits as (
  select 
    browser_id, 
    event_source, 
    new_visitor,
      isit_id 
from etsy-data-warehouse-dev.madelinecollins.boe_first_visits 
  where visit_rnk = 1 
)
, event_counts as (
  SELECT
    e.event_type as event_name,
    v.visit_id,
    v.browser_id,
    v.event_source,
    v.new_visitor
  FROM `etsy-data-warehouse-prod.weblog.events` e
  JOIN first_browser_visits v 
      using (visit_id)
  WHERE event_type in (
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

    --Favorites Quiz
    'onboarding_favorites-q4-2019_viewed',
    'onboarding_faves_tapped_skip',
    'favorites_onboarding_done_button_tapped',

    'homescreen',
    'boe_homescreen_tab_delivered',
    'recommendations_module_seen'
  )
  GROUP BY event_type
)

SELECT
  event_counts.event_name,
  CASE 
    WHEN event_counts.event_name IN (
      'sign_in_screen',
      'continue_as_guest_tapped',
      'login_view',
      'BOE_social_sign_in_tapped',
      'BOE_etsy_sign_in_tapped'
    ) THEN '1 - Log In Splash Screen'
    WHEN event_counts.event_name IN (
      'join_submit',
      'BOE_email_sign_in_webview_cancelled'
    ) THEN '2 - Registration Web View'
    WHEN event_counts.event_name IN (
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
      'BOE_email_sign_in_webview_cancelled'
    ) THEN '3 - Sign In Web View'
    WHEN event_counts.event_name IN (
      'login_view',
      'BOE_social_sign_in_tapped'
    ) THEN '4 - Sign In from Homescreen'
    WHEN event_counts.event_name IN (
      'notification_registration_interstitial_enable_tapped',
      'notification_registration_interstitial_dismiss_tapped',
      'update_setting',
      'notification_registration_interstitial_viewed'
    ) THEN '5 - Notifications Opt In pt 1'
    WHEN event_counts.event_name IN (
      'push_prompt_permission_granted',
      'push_prompt_permission_denied',
      'app_tracking_transparency_system_prompt_denied_tapped',
      'app_tracking_transparency_system_prompt_authorized_tapped'
    ) THEN '6 - Notifications Opt In pt 2'
    WHEN event_counts.event_name IN (
      'onboarding_favorites-q4-2019_viewed',
      'onboarding_faves_tapped_skip',
      'favorites_onboarding_done_button_tapped'
    ) THEN '7 - Favorites Quiz'
    WHEN event_counts.event_name IN (
      'homescreen',
      'boe_homescreen_tab_delivered',
      'recommendations_module_seen'
    ) THEN '8 - Initial Content'
    ELSE NULL END as screen,
  event_counts.total_events,
  event_counts.distinct_browsers,
  (event_counts.distinct_browsers/totals.distinct_browsers)*100 as pct_of_browsers,
  event_counts.total_events/totals.distinct_browsers as mean,
  event_counts.mean as mean_of_users_with_event
FROM totals,event_counts
ORDER BY screen




--------------------------------------------------------------------------------------------------------
--breakdown engagements in first visit
  -------add in collect, engagement metric, convert, guest convert, etc
--------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.boe_first_visit_actions` AS (
with first_visits as (
select
  * 
from `etsy-data-warehouse-dev.madelinecollins.boe_first_visits` 
where 
  visit_rnk =1
  and _date between current_date -30 and current_date-1
)
, events as (
select
  visit_id
  --tab engagement
  , max(case when event_type in ('homescreen') then 1 else 0 end) as saw_home
  , max(case when event_type in ('deals_tab_delivered','app_deals') then 1 else 0 end) as saw_deals
  , max(case when event_type in ('gift_mode_home') then 1 else 0 end) as saw_gift_mode_home
  , max(case when event_type in ('favorites_and_lists','view_favorites_landing_complementary') then 1 else 0 end) as saw_favorites_tab -- includes favorites event from experiment
  , max(case when event_type in ('cart_view') then 1 else 0 end) as saw_cart_view
  , max(case when event_type in ('browselistings') then 1 else 0 end) as saw_browselistings -- search 
  , max(case when event_type in ('view_listing') then 1 else 0 end) as saw_view_listings 
  , max(case when event_type in ('shop_home') then 1 else 0 end) as saw_shop_home  
  , max(case when event_type in ('add_to_cart') then 1 else 0 end) as saw_add_to_cart
  , max(case when event_type in ('checkout_start') then 1 else 0 end) as saw_checkout_start
  , max(case when event_type in ('you_tab_viewed','you_screen','you') then 1 else 0 end) as saw_profile -- added in 'you' b
--onboarding events 
  , max(case when event_type = "sign_in_screen" then 1 else 0 end) as sign_in_screen --primary event
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
