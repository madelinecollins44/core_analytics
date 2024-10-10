--this query looks at which log in event successfully got a user to the next register or sign in page 
--browsers might be duped if they have multiple attempts at signing in 
--also, homescreen is not included at the moment due to duped events 
with all_login_events as (
  select 
  a.browser_id,
  b.visit_id,
  b.event_name,
  a.new_visitor,
  lead(event_name) over (partition by browser_id order by sequence_number) as next_event,
  b.sequence_number
from etsy-data-warehouse-dev.madelinecollins.boe_first_visits a
inner join etsy-data-warehouse-dev.madelinecollins.app_onboarding_events b using (visit_id)
where visit_rnk = 1 --pull out first visit 
  and _date >= current_date-30
  and event_source in ('ios') -- only looking at ios 
  and event_name in ( --only pull in login/ screen related 
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
      --Sign in Homescreen
      -- 'login_view',
      -- 'BOE_social_sign_in_tapped',
    --log in events 
    'continue_as_guest_tapped',
    'BOE_social_sign_in_tapped',
    'BOE_etsy_sign_in_tapped'
))
, great_success as (
select
  browser_id
  , visit_id
  , event_name
  , new_visitor
  ,  CASE 
      WHEN next_event  IN (
        'join_submit',
        'BOE_email_sign_in_webview_cancelled'
      ) THEN '2 - Registration Web View'
      WHEN next_event  IN (
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
        'login_view',
        'BOE_social_sign_in_tapped'
      ) THEN '3 - Sign In Web View or Sign In from Homescreen'
      -- WHEN next_event  IN (
      --   'login_view',
      --   'BOE_social_sign_in_tapped'
      -- ) THEN '4 - Sign In from Homescreen'
      ELSE NULL END as next_screen
  , case when next_event in (
      'join_submit',
      'BOE_email_sign_in_webview_cancelled',
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
    ) then 1 
    else 0 end as successful_click
FROM all_login_events
WHERE event_name IN (
  'continue_as_guest_tapped',
  'BOE_social_sign_in_tapped',
  'BOE_etsy_sign_in_tapped')
)
select
  event_name, 
  new_visitor,
  next_screen,
  count(distinct browser_id) as browsers
from great_success
where successful_click=1
group by all 

------------------------------------------------
--which social sign ins are most popular
------------------------------------------------
with all_signin_events as (
select
  v.browser_id,
  e.visit_id,
  e.sequence_number,
  beacon.event_name as event_name,
  (select value from unnest(beacon.properties.key_value) where key = "platform") as platform,
from 
  etsy-data-warehouse-dev.madelinecollins.boe_first_visits v
inner join 
  etsy-visit-pipe-prod.canonical.visit_id_beacons e using (visit_id)
where 
  date(_partitiontime) >= current_date-30 
  and visit_rnk = 1 --pull out first visit 
  and event_source in ('ios') 
  and v._date >= current_date-30
  and beacon.event_name in ('BOE_social_sign_in_tapped','BOE_etsy_sign_in_tapped')
)
select
  count(distinct case when event_name in ('BOE_social_sign_in_tapped') and platform in ('apple') then browser_id end) as apple_sign_ins,
  count(distinct case when event_name in ('BOE_social_sign_in_tapped') and platform in ('google') then browser_id end) as google_sign_ins,
  count(distinct case when event_name in ('BOE_social_sign_in_tapped') and platform in ('facebook') then browser_id end) as facebook_sign_ins,
  count(distinct case when event_name in ('BOE_etsy_sign_in_tapped') then browser_id end) as etsy_sign_ins
from all_signin_events

