with first_visits as (
select
  browser_id
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
)
, engagements as (
select
  --signed in 
  count(distinct case when signed_in > 0 then browser_id else 0 end) as signed_in_browsers 
  , avg(case when signed_in > 0 then visit_duration / (1000 * 60) else 0 end) as signed_in_avg_visit_duration
  , sum(case when signed_in > 0 then v.converted else 0 end) as signed_in_conversions
  , sum(case when signed_in > 0 then v.total_gms else 0 end) as signed_in_total_gms
  , sum(case when signed_in > 0 then v.bounced else 0 end) as signed_in_bounces
  , sum(case when signed_in > 0 then v.total_gms else 0 end)/sum(case when signed_in > 0 then v.converted else 0 end) as signed_in_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and signed_in > 0 then v.visit_id end) as signed_in_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and signed_in > 0 then v.visit_id end) as signed_in_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and signed_in > 0
      then v.visit_id end) as signed_in_engaged_visits
  , sum(case when signed_in > 0 then lv.listing_views) as signed_in_listing_views
  --registered
  count(distinct case when registered > 0 then browser_id end) as registered_browsers 
  , avg(case when registered > 0 then visit_duration / (1000 * 60)) as registered_avg_visit_duration
  , sum(case when registered > 0 then v.converted) as registered_conversions
  , sum(case when registered > 0 then v.total_gms) as registered_total_gms
  , sum(case when registered > 0 then v.bounced) as registered_bounces
  , sum(case when registered > 0 then v.total_gms)/sum(case when registered > 0 then v.converted) as registered_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and registered > 0 then v.visit_id end) as registered_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and registered > 0 then v.visit_id end) as registered_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and registered > 0
      then v.visit_id end) as registered_engaged_visits
  , sum(case when registered > 0 then lv.listing_views) as registered_listing_views
  --completes_favorites_quiz
  count(distinct case when completes_favorites_quiz > 0 then browser_id end) as completes_favorites_quiz_browsers 
  , avg(case when completes_favorites_quiz > 0 then visit_duration / (1000 * 60)) as completes_favorites_quiz_avg_visit_duration
  , sum(case when completes_favorites_quiz > 0 then v.converted) as completes_favorites_quiz_conversions
  , sum(case when completes_favorites_quiz > 0 then v.total_gms) as completes_favorites_quiz_total_gms
  , sum(case when completes_favorites_quiz > 0 then v.bounced) as completes_favorites_quiz_bounces
  , sum(case when completes_favorites_quiz > 0 then v.total_gms)/sum(case when completes_favorites_quiz > 0 then v.converted) as completes_favorites_quiz_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and completes_favorites_quiz > 0 then v.visit_id end) as completes_favorites_quiz_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and completes_favorites_quiz > 0 then v.visit_id end) as completes_favorites_quiz_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and completes_favorites_quiz > 0
      then v.visit_id end) as completes_favorites_quiz_engaged_visits
  , sum(case when completes_favorites_quiz > 0 then lv.listing_views) as completes_favorites_quiz_listing_views--apple
  --apple_signed_in
  count(distinct case when apple_signed_in > 0 then browser_id end) as apple_signed_in_browsers 
  , avg(case when apple_signed_in > 0 then visit_duration / (1000 * 60)) as apple_signed_in_avg_visit_duration
  , sum(case when apple_signed_in > 0 then v.converted) as apple_signed_in_conversions
  , sum(case when apple_signed_in > 0 then v.total_gms) as apple_signed_in_total_gms
  , sum(case when apple_signed_in > 0 then v.bounced) as apple_signed_in_bounces
  , sum(case when apple_signed_in > 0 then v.total_gms)/sum(case when apple_signed_in > 0 then v.converted) as apple_signed_in_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and apple_signed_in > 0 then v.visit_id end) as apple_signed_in_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and apple_signed_in > 0 then v.visit_id end) as apple_signed_in_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and apple_signed_in > 0
      then v.visit_id end) as apple_signed_in_engaged_visits
  , sum(case when apple_signed_in > 0 then lv.listing_views) as apple_signed_in_listing_views
  --fb_signed_in
  count(distinct case when fb_signed_in > 0 then browser_id end) as fb_signed_in_browsers 
  , avg(case when fb_signed_in > 0 then visit_duration / (1000 * 60)) as fb_signed_in_avg_visit_duration
  , sum(case when fb_signed_in > 0 then v.converted) as fb_signed_in_conversions
  , sum(case when fb_signed_in > 0 then v.total_gms) as fb_signed_in_total_gms
  , sum(case when fb_signed_in > 0 then v.bounced) as fb_signed_in_bounces
  , sum(case when fb_signed_in > 0 then v.total_gms)/sum(case when fb_signed_in > 0 then v.converted) as fb_signed_in_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and fb_signed_in > 0 then v.visit_id end) as fb_signed_in_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and fb_signed_in > 0 then v.visit_id end) as fb_signed_in_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and fb_signed_in > 0
      then v.visit_id end) as fb_signed_in_engaged_visits
  , sum(case when fb_signed_in > 0 then lv.listing_views) as fb_signed_in_listing_views
  --google_signed_in
  count(distinct case when google_signed_in > 0 then browser_id end) as google_signed_in_browsers 
  , avg(case when google_signed_in > 0 then visit_duration / (1000 * 60)) as google_signed_in_avg_visit_duration
  , sum(case when google_signed_in > 0 then v.converted) as google_signed_in_conversions
  , sum(case when google_signed_in > 0 then v.total_gms) as google_signed_in_total_gms
  , sum(case when google_signed_in > 0 then v.bounced) as google_signed_in_bounces
  , sum(case when google_signed_in > 0 then v.total_gms)/sum(case when google_signed_in > 0 then v.converted) as google_signed_in_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and google_signed_in > 0 then v.visit_id end) as google_signed_in_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and google_signed_in > 0 then v.visit_id end) as google_signed_in_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and google_signed_in > 0
      then v.visit_id end) as google_signed_in_engaged_visits
  , sum(case when google_signed_in > 0 then lv.listing_views) as google_signed_in_listing_views
    --etsy_signed_in
  count(distinct case when etsy_signed_in > 0 then browser_id end) as etsy_signed_in_browsers 
  , avg(case when etsy_signed_in > 0 then visit_duration / (1000 * 60)) as etsy_signed_in_avg_visit_duration
  , sum(case when etsy_signed_in > 0 then v.converted) as etsy_signed_in_conversions
  , sum(case when etsy_signed_in > 0 then v.total_gms) as etsy_signed_in_total_gms
  , sum(case when etsy_signed_in > 0 then v.bounced) as etsy_signed_in_bounces
  , sum(case when etsy_signed_in > 0 then v.total_gms)/sum(case when etsy_signed_in > 0 then v.converted) as etsy_signed_in_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and etsy_signed_in > 0 then v.visit_id end) as etsy_signed_in_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and etsy_signed_in > 0 then v.visit_id end) as etsy_signed_in_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and etsy_signed_in > 0
      then v.visit_id end) as etsy_signed_in_engaged_visits
  , sum(case when etsy_signed_in > 0 then lv.listing_views) as etsy_signed_in_listing_views
  from first_visits f
left join events e 
  on f.visit_id = e.visit_id
left join `etsy-data-warehouse-prod.user_mart.mapped_user_profile` m
  on f.user_id = m.mapped_user_id
);
