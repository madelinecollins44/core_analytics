  --this rollup looks at first boe visit ever from each user (regardless of the browser_id). used user level bc of kristis query in looker

  create or replace table etsy-data-warehouse-dev.madelinecollins.users_first_boe_visit as (
  with app_visits as (
    SELECT DISTINCT
      u.mapped_user_id,
      v.visit_id,
      v.region,
      browser_id,
      browser_platform, 
      platform,
      v.start_datetime,
    FROM `etsy-data-warehouse-prod.weblog.visits` v
    LEFT JOIN `etsy-data-warehouse-prod.user_mart.user_mapping` u on v.user_id = u.user_id -- want to also include anyone that was signed out during download time 
    WHERE
      v.event_source in ('ios','android')
      -- AND v.app_name in ('ios-EtsyInc','android-EtsyInc','ios-ButterSellOnEtsy', 'android-ButterSellOnEtsy') -- only looking at boe downloads 
      AND v._date >= '2022-01-01' and v._date < current_date
      AND landing_event != "account_credit_card_settings" -- filter out visits that start on the CC settings page. there was an attack inflating "downloads" in August 2021
      and platform in ('boe')
  )
  , first_visits as (
  SELECT
    mapped_user_id,
    buyer_segment,
    case when mapped_user_id is not null then 1 else 0 end as signed_in,
    visit_id,
    a.region,  
    browser_platform,
    start_datetime,
  FROM app_visits a
  left join etsy-data-warehouse-prod.user_mart.mapped_user_profile b using (mapped_user_id) 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY mapped_user_id ORDER BY start_datetime) = 1 -- this only looks at first app for each USER, not each user across different devices 
  )
  , yy_union as (
    select
    --ty calcs
      date(timestamp(start_datetime)) as _date,
      'ty' as era, 
      buyer_segment, 
      signed_in,
      region,  
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
      region,  
      browser_platform,
      count(distinct visit_id) as downloads,
      count(distinct mapped_user_id) as user_downloads
      from first_visits
  group by all 
  )
  select
    _date,
    buyer_segment, 
    signed_in,
    region,  
    browser_platform,
    sum(case when era= 'ty' then downloads end) as ty_downloads_visit_level,
    sum(case when era= 'ty' then user_downloads end) as ty_downloads_user_level,
    sum(case when era= 'ly' then downloads end) as ly_downloads_visit_level,
    sum(case when era= 'ly' then user_downloads end) as ly_downloads_user_level,
  from yy_union
  group by all 
  );
