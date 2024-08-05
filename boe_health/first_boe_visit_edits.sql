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



----------testing 
-- select * from etsy-data-warehouse-dev.madelinecollins.first_app_visit_test where first_boe_buyer_segment is not null 

select
start_datetime, visit_id
from etsy-data-warehouse-prod.weblog.visits v
INNER JOIN `etsy-data-warehouse-prod.user_mart.user_mapping` u on v.user_id = u.user_id
where 
(mapped_user_id = 183022571 or mapped_user_id = 73365265 or mapped_user_id = 816204040)
and v._date >= current_date-15
and app_name in ('ios-EtsyInc','android-EtsyInc') 
QUALIFY ROW_NUMBER() OVER (PARTITION BY mapped_user_id ORDER BY start_datetime) = 1

--mapped user id, first visit id 
--183022571, 2FF477C5A31E40189F435B4B0464.1721870848611.1
--73365265, vR40UyEoRySgO_GLCVpogA.1721753149139.1
--816204040, 17BDEB80CF2D47078BE999B7CEB1.1721576281616.1

---test to make sure everything is unique 
-- select mapped_user_id, count(*) from `etsy-data-warehouse-dev.madelinecollins.first_app_visit_test` group by 1 order by 2 desc

select visit_id, count(*) from etsy-bigquery-adhoc-prod._script0c341a53b6e44e0daeb33014dad616dc9c1d50f0.first_app_visits group by all order by 2 desc
-- 1

select mapped_user_id, count(*) from etsy-bigquery-adhoc-prod._script0c341a53b6e44e0daeb33014dad616dc9c1d50f0.first_app_visits group by all order by 2 desc
-- 2
-- saw that some user_ids have 2 entries-- realized needed to look at app_type as well
select * from etsy-bigquery-adhoc-prod._script0c341a53b6e44e0daeb33014dad616dc9c1d50f0.first_app_visits where mapped_user_id = 764158222

select mapped_user_id, app_type, count(*) from etsy-bigquery-adhoc-prod._script0c341a53b6e44e0daeb33014dad616dc9c1d50f0.first_app_visits group by all order by 3 desc
-- 2 
 
