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
  count(distinct case when signed_in > 0 then browser_id end) as signed_in_browsers 
  , avg(case when signed_in > 0 then visit_duration / (1000 * 60)) as signed_in_avg_visit_duration
  , sum(case when signed_in > 0 then v.converted) as signed_in_conversions
  , sum(case when signed_in > 0 then v.total_gms) as signed_in_total_gms
  , sum(case when signed_in > 0 then v.bounced) as signed_in_bounces
  , sum(case when signed_in > 0 then v.total_gms)/sum(case when signed_in > 0 then v.converted) as signed_in_acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300 and signed_in > 0 then v.visit_id end) as signed_in_visits_5_min
  , count(distinct case when v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 and signed_in > 0 then v.visit_id end) as signed_in_collected_visits
  , count(distinct case 
      when (timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0)
      and signed_in > 0
      then v.visit_id end) as signed_in_engaged_visits
  , sum(case when signed_in > 0 then lv.listing_views) as signed_in_listing_views
from first_visits f
left join events e 
  on f.visit_id = e.visit_id
left join `etsy-data-warehouse-prod.user_mart.mapped_user_profile` m
  on f.user_id = m.mapped_user_id
);
