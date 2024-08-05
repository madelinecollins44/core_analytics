-- owner: jnanji@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: as a proxy to app downloads, this rollups identifies the first BOE and ESA visit for each user.


-- first visits from an apps
CREATE OR REPLACE TEMP TABLE first_app_visits as (
  with app_visits as (
    SELECT DISTINCT
      u.mapped_user_id,
      v.visit_id,
      canonical_region,
      v.event_source as os,
      case
        when app_name in ('ios-EtsyInc','android-EtsyInc') then 'boe'
        else 'esa' end as app_type,
      v.start_datetime,
      s.buyer_segment
    FROM `etsy-data-warehouse-prod.weblog.visits` v
    INNER JOIN `etsy-data-warehouse-prod.user_mart.user_mapping` u on v.user_id = u.user_id
    LEFT JOIN etsy-data-warehouse-prod.rollups.buyer_segmentation_vw s 
      on u.mapped_user_id=s.mapped_user_id
      and v._date=s.as_of_date -- gets buyer segment as of download date
    WHERE
      v.event_source in ('ios','android')
      AND v.app_name in ('ios-EtsyInc','android-EtsyInc','ios-ButterSellOnEtsy', 'android-ButterSellOnEtsy')
      AND v._date < current_date()
      and v._date >= current_date-15
      AND u.mapped_user_id is not null
      AND landing_event != "account_credit_card_settings" -- filter out visits that start on the CC settings page. there was an attack inflating "downloads" in August 2021
  )
  SELECT
    mapped_user_id,
    app_type,
    visit_id,
    canonical_region,  
    os,
    start_datetime,
    buyer_segment
  FROM app_visits  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY mapped_user_id, app_type ORDER BY start_datetime) = 1
);


CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.madelinecollins.first_app_visit_test` AS (
  SELECT
    mapped_user_id,
    min(case when app_type = 'boe' then canonical_region end) as first_boe_canonical_region,
    min(case when app_type = 'boe' then os end) as first_boe_os,
    min(case when app_type = 'boe' then start_datetime end) as first_boe_visit_datetime,
    min(case when app_type = 'boe' then visit_id end) as first_boe_visit_id,
    min(case when app_type = 'boe' then buyer_segment end) as first_boe_buyer_segment,
    min(case when app_type = 'esa' then canonical_region end) as first_esa_canonical_region,
    min(case when app_type = 'esa' then os end) as first_esa_os,
    min(case when app_type = 'esa' then start_datetime end) as first_esa_visit_datetime,
    min(case when app_type = 'esa' then visit_id end) as first_esa_visit_id
  FROM first_app_visits
  GROUP BY 1
);
