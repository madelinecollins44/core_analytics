------------------------------------------------------------
--kristis method
------------------------------------------------------------
WITH first_app AS (
  SELECT
      user_id,
      case when app_name = "ios-EtsyInc" then 'iOS'
        when app_name = "android-EtsyInc" then 'Android'
        else 'Unknown' end as OS,
      canonical_region,
      min(run_date) AS first_app
    FROM
      `etsy-data-warehouse-prod`.weblog.visits
    WHERE _date >= "2010-01-01"
    and platform = "boe"
    and app_name in ("ios-EtsyInc","android-EtsyInc")
    and user_id > 0
    -- filter out visits that start on the CC settings page
    -- there was an attack inflating "downloads" in August 2021
    and landing_event != "account_credit_card_settings"
    GROUP BY all
), yy_union AS (
  SELECT
      'ty' AS era,
      DATE(timestamp_seconds(first_app.first_app)) AS date,
      OS,
      canonical_region,
      count(*) AS users
    FROM
      first_app
    GROUP BY all
  UNION ALL
  SELECT
      'ly' AS era,
      CAST(date_add(DATE(timestamp_seconds(first_app_0.first_app)), interval 52 WEEK) as DATETIME) AS date,
      OS,
      canonical_region,
      count(*) AS users
    FROM
      first_app AS first_app_0
    GROUP BY all
  ORDER BY
    1 NULLS LAST,
    2, 3, 4
)
SELECT
    yy_union.date,
    OS,
    canonical_region,
    sum(CASE
      WHEN yy_union.era = 'ty' THEN yy_union.users
    END) AS ty_downloads,
    sum(CASE
      WHEN yy_union.era = 'ly' THEN yy_union.users
    END) AS ly_downloads
  FROM
    yy_union
  WHERE yy_union.date < CAST(current_date() as DATETIME)
  GROUP BY all
ORDER BY
  1,2, 3

------------------------------------------------------------
--using first download date
------------------------------------------------------------
--find first boe visit 
create or replace temporary table app_downloads as (
with all_boe_visits as (
select
 _date 
  , visit_id
  , user_id
  , browser_id
  , platform
  , browser_platform
  , top_channel
  , region
  , row_number() over (partition by user_id order by unix_seconds(timestamp (_date)) desc) AS visit_order
from 
  etsy-data-warehouse-prod.weblog.visits
where 
  platform in ('boe')
and _date >= current_date-365
)
select
  *
from all_boe_visits 
where visit_order =1
); 

------------------------------------------------------------
--using marketings app download tables
------------------------------------------------------------
with all_downloads as (
--organic installs 
select
  Country_Code
  , date(Event_Time) as download_date
  , platform
  , advertising_id 
  , 'organic' as download_type
from
  etsy-data-warehouse-prod.marketing.appsflyer_organic_installs -- organic app 
union all
-- paid downloads 
select
  Country_Code
  , date(Event_Time) as download_date
  , platform
  , advertising_id 
  , 'paid' as download_type
from
  etsy-data-warehouse-prod.marketing.appsflyer_paid_installs 
) 
select 
  downloads.country_code
  , downloads.download_date
  , downloads.platform
  , downloads.download_type
  , segments.buyer_segment
  , downloads.advertising_id
from 
  all_downloads downloads
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile segments 
    on downloads.advertising_id=cast(segments.mapped_user_id as string)
