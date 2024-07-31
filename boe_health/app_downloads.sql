--this rollup looks at first boe visit ever from each user (regardless of the browser_id)
with app_visits as (
    SELECT DISTINCT
      u.mapped_user_id,
      v.visit_id,
      canonical_region,
      browser_id,
      browser_platform, 
      platform,
      -- v.event_source as os,
      -- case
      --   when app_name in ('ios-EtsyInc','android-EtsyInc') then 'boe'
      --   else 'esa' end as app_type,
      v.start_datetime,
    FROM `etsy-data-warehouse-prod.weblog.visits` v
    LEFT JOIN `etsy-data-warehouse-prod.user_mart.user_mapping` u on v.user_id = u.user_id -- want to also include anyone that was signed out during download time 
    WHERE
      v.event_source in ('ios','android')
      -- AND v.app_name in ('ios-EtsyInc','android-EtsyInc','ios-ButterSellOnEtsy', 'android-ButterSellOnEtsy') -- only looking at boe downloads 
      AND v._date >= '2022-01-01'
      AND landing_event != "account_credit_card_settings" -- filter out visits that start on the CC settings page. there was an attack inflating "downloads" in August 2021
      and platform in ('boe')
  )
  , first_visits as (
  SELECT
    mapped_user_id,
    buyer_segment,
    case when mapped_user_id is not null then 1 else 0 end as signed_in,
    app_type,
    visit_id,
    canonical_region,  
    -- os,
    browser_platform,
    start_datetime,
  FROM app_visits 
  left join etsy-data-warehouse-prod.user_mart.mapped_user_profile using (mapped_user_id) 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY mapped_user_id ORDER BY start_datetime) = 1 -- this only looks at first app for each USER, not each user across different devices 
  ), yy_union as (
    select
    --ty calcs
      date(timestamp(start_datetime)) as _date,
      'ty' as era, 
      buyer_segment, 
      signed_in,
      -- app_type,
      canonical_region,  
      -- os,
      browser_platform,
      count(distinct visit_id) as downloads,
      count(distinct mapped_user_id) as user_downloads
  from first_visits
  group by all 
  union all 
  -- ly calcs 
  select
     date_add(date(timestamp(start_datetime)), interval 52 WEEK) as _date,
     'ly' as era, 
      buyer_segment, 
      signed_in,
      -- app_type,
      canonical_region,  
      -- os,
      browser_platform,
      count(distinct visit_id) as downloads,
      count(distinct mapped_user_id) as user_downloads
      from first_visits
  group by all 
  )
  select
    _date,
    era, 
    buyer_segment, 
    signed_in,
    -- app_type,
    canonical_region,  
    -- os,
    browser_platform,
    sum(case when era= 'ty' then downloads end) as ty_downloads_visit_level,
    sum(case when era= 'ty' then user_downloads end) as ty_downloads_user_level,
    sum(case when era= 'ly' then downloads end) as ty_downloads_visit_level,
    sum(case when era= 'ly' then user_downloads end) as ty_downloads_user_level,
  from yy_union
  group by all 
