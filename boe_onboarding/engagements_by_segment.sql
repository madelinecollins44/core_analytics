with first_visits as (
select
  browser_id
  , a.visit_id
  , max(case when beacon.event_name = "signin_submit" then 1 else 0 end) as signed_in
  , max(case when beacon.event_name = "join_submit" then 1 else 0 end) as registered
  , max(case when beacon.event_name = "favorites_onboarding_done_button_tapped" then 1 else 0 end) as completes_favorites_quiz 
  , max(case when beacon.event_name in ('BOE_social_sign_in_tapped') and platform in ('apple') then 1 else 0 end) as apple_signed_in
  , max(case when beacon.event_name in ('BOE_social_sign_in_tapped') and platform in ('facebook') then 1 else 0 end) as fb_signed_in 
  , max(case when beacon.event_name in ('BOE_social_sign_in_tapped') and platform in ('google') then 1 else 0 end) as google_signed_in 
  , max(case when beacon.event_name in ('BOE_etsy_sign_in_tapped') then 1 else 0 end) as etsy_signed_in  
from `etsy-data-warehouse-dev.madelinecollins.boe_first_visits` a
inner join etsy-visit-pipe-prod.canonical.visit_id_beacons e 
  using (visit_id)
where 
  date(_partitiontime) >= current_date-30 
  and beacon.event_name in ('BOE_social_sign_in_tapped','BOE_etsy_sign_in_tapped','signin_submit','favorites_onboarding_done_button_tapped','join_submit')  
  and a.visit_rnk =1
  and a._date >= current_date-30
group by all
)
select
  count(distinct f.browser_id) as total_browsers
  --signed in 
  , count(distinct case when signed_in > 0 then f.browser_id end) as signed_in_browsers 
  , avg(case when signed_in > 0 then visit_duration / (1000 * 60) end) as signed_in_avg_visit_duration
  , sum(case when signed_in > 0 then v.converted end) as signed_in_conversions
  , sum(case when signed_in > 0 then v.total_gms end) as signed_in_total_gms
  , sum(case when signed_in > 0 then v.bounced end) as signed_in_bounces
  , sum(case when signed_in > 0 then v.total_gms end)/sum(case when signed_in > 0 then v.converted end) as signed_in_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and signed_in > 0 then f.browser_id end) as signed_in_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and signed_in > 0 then f.browser_id end) as signed_in_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and signed_in > 0
      then f.browser_id end) as signed_in_engaged_visits
  , count(case when signed_in > 0 then lv.listing_id end) as signed_in_listing_views
  --registered
  , count(distinct case when f.registered > 0 then f.browser_id end) as registered_browsers 
  , avg(case when f.registered > 0 then visit_duration / (1000 * 60) end) as registered_avg_visit_duration
  , sum(case when f.registered > 0 then v.converted end) as registered_conversions
  , sum(case when f.registered > 0 then v.total_gms end) as registered_total_gms
  , sum(case when f.registered > 0 then v.bounced end) as registered_bounces
  , sum(case when f.registered > 0 then v.total_gms end)/sum(case when f.registered > 0 then v.converted end) as registered_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and f.registered > 0 then f.browser_id end) as registered_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and f.registered > 0 then f.browser_id end) as registered_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and f.registered > 0
      then f.browser_id end) as registered_engaged_visits
  , count(case when f.registered > 0 then lv.listing_id end) as registered_listing_views
  --completes_favorites_quiz
  , count(distinct case when completes_favorites_quiz > 0 then f.browser_id end) as completes_favorites_quiz_browsers 
  , avg(case when completes_favorites_quiz > 0 then visit_duration / (1000 * 60) end) as completes_favorites_quiz_avg_visit_duration
  , sum(case when completes_favorites_quiz > 0 then v.converted end) as completes_favorites_quiz_conversions
  , sum(case when completes_favorites_quiz > 0 then v.total_gms end) as completes_favorites_quiz_total_gms
  , sum(case when completes_favorites_quiz > 0 then v.bounced end) as completes_favorites_quiz_bounces
  , sum(case when completes_favorites_quiz > 0 then v.total_gms end)/sum(case when completes_favorites_quiz > 0 then v.converted end) as completes_favorites_quiz_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and completes_favorites_quiz > 0 then f.browser_id end) as completes_favorites_quiz_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and completes_favorites_quiz > 0 then f.browser_id end) as completes_favorites_quiz_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and completes_favorites_quiz > 0
      then f.browser_id end) as completes_favorites_quiz_engaged_visits
  , count(case when completes_favorites_quiz > 0 then lv.listing_id end) as completes_favorites_quiz_listing_views--apple
  --apple_signed_in
  , count(distinct case when apple_signed_in > 0 then f.browser_id end) as apple_signed_in_browsers 
  , avg(case when apple_signed_in > 0 then visit_duration / (1000 * 60) end) as apple_signed_in_avg_visit_duration
  , sum(case when apple_signed_in > 0 then v.converted end) as apple_signed_in_conversions
  , sum(case when apple_signed_in > 0 then v.total_gms end) as apple_signed_in_total_gms
  , sum(case when apple_signed_in > 0 then v.bounced end) as apple_signed_in_bounces
  , sum(case when apple_signed_in > 0 then v.total_gms end)/sum(case when apple_signed_in > 0 then v.converted end) as apple_signed_in_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and apple_signed_in > 0 then f.browser_id end) as apple_signed_in_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and apple_signed_in > 0 then f.browser_id end) as apple_signed_in_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and apple_signed_in > 0
      then f.browser_id end) as apple_signed_in_engaged_visits
  , count(case when apple_signed_in > 0 then lv.listing_id end) as apple_signed_in_listing_views
  --fb_signed_in
  , count(distinct case when fb_signed_in > 0 then f.browser_id end) as fb_signed_in_browsers 
  , avg(case when fb_signed_in > 0 then visit_duration / (1000 * 60) end) as fb_signed_in_avg_visit_duration
  , sum(case when fb_signed_in > 0 then v.converted end) as fb_signed_in_conversions
  , sum(case when fb_signed_in > 0 then v.total_gms end) as fb_signed_in_total_gms
  , sum(case when fb_signed_in > 0 then v.bounced end) as fb_signed_in_bounces
  , sum(case when fb_signed_in > 0 then v.total_gms end)/sum(case when fb_signed_in > 0 then v.converted end) as fb_signed_in_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and fb_signed_in > 0 then f.browser_id end) as fb_signed_in_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and fb_signed_in > 0 then f.browser_id end) as fb_signed_in_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and fb_signed_in > 0
      then f.browser_id end) as fb_signed_in_engaged_visits
  , count(case when fb_signed_in > 0 then lv.listing_id end) as fb_signed_in_listing_views
  --google_signed_in
  , count(distinct case when google_signed_in > 0 then f.browser_id end) as google_signed_in_browsers 
  , avg(case when google_signed_in > 0 then visit_duration / (1000 * 60) end) as google_signed_in_avg_visit_duration
  , sum(case when google_signed_in > 0 then v.converted end) as google_signed_in_conversions
  , sum(case when google_signed_in > 0 then v.total_gms end) as google_signed_in_total_gms
  , sum(case when google_signed_in > 0 then v.bounced end) as google_signed_in_bounces
  , sum(case when google_signed_in > 0 then v.total_gms end)/sum(case when google_signed_in > 0 then v.converted end) as google_signed_in_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and google_signed_in > 0 then f.browser_id end) as google_signed_in_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and google_signed_in > 0 then f.browser_id end) as google_signed_in_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and google_signed_in > 0
      then f.browser_id end) as google_signed_in_engaged_visits
  , count(case when google_signed_in > 0 then lv.listing_id end) as google_signed_in_listing_views
    --etsy_signed_in
  , count(distinct case when etsy_signed_in > 0 then f.browser_id end) as etsy_signed_in_browsers 
  , avg(case when etsy_signed_in > 0 then visit_duration / (1000 * 60) end) as etsy_signed_in_avg_visit_duration
  , sum(case when etsy_signed_in > 0 then v.converted end) as etsy_signed_in_conversions
  , sum(case when etsy_signed_in > 0 then v.total_gms end) as etsy_signed_in_total_gms
  , sum(case when etsy_signed_in > 0 then v.bounced end) as etsy_signed_in_bounces
  , sum(case when etsy_signed_in > 0 then v.total_gms end)/sum(case when etsy_signed_in > 0 then v.converted end) as etsy_signed_in_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and etsy_signed_in > 0 then f.browser_id end) as etsy_signed_in_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and etsy_signed_in > 0 then f.browser_id end) as etsy_signed_in_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and etsy_signed_in > 0
      then f.browser_id end) as etsy_signed_in_engaged_visits
  , count(case when etsy_signed_in > 0 then lv.listing_id end) as etsy_signed_in_listing_views
from first_visits f
inner join etsy-data-warehouse-prod.weblog.visits v using (visit_id)
left join etsy-data-warehouse-prod.analytics.listing_views lv on lv.visit_id=f.visit_id
where 
  v._date >= "2017-01-01" 
  and lv._date >= '2017-01-01'
group by all 
