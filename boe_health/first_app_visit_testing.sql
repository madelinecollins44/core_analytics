-- try adding in signed out here
-- owner: jnanji@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: as a proxy to app downloads, this rollups identifies the first BOE and ESA visit for each user.

-- first visits from an apps
CREATE OR REPLACE TEMP TABLE first_app_visits as (
  with app_visits as (
    SELECT DISTINCT
      u.mapped_user_id,
      v.visit_id,
      v.user_id, 
      canonical_region,
      v.event_source as os,
      case
        when app_name in ('ios-EtsyInc','android-EtsyInc') then 'boe'
        else 'esa' end as app_type,
      v.start_datetime,
      v._date
    FROM `etsy-data-warehouse-prod.weblog.visits` v
    left JOIN `etsy-data-warehouse-prod.user_mart.user_mapping` u on v.user_id = u.user_id
    WHERE
      v.event_source in ('ios','android')
      AND v.app_name in ('ios-EtsyInc','android-EtsyInc','ios-ButterSellOnEtsy', 'android-ButterSellOnEtsy')
      AND v._date < current_date()
      -- AND u.mapped_user_id is not null
      AND landing_event != "account_credit_card_settings" -- filter out visits that start on the CC settings page. there was an attack inflating "downloads" in August 2021
      and v._date >= current_date-7 -- testing w dates for ease
  )
  SELECT
    mapped_user_id,
    user_id,
    app_type,
    visit_id,
    canonical_region,  
    os,
    start_datetime,
    _date
  FROM app_visits  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY mapped_user_id, app_type ORDER BY start_datetime) = 1
);

CREATE OR REPLACE TEMP TABLE buyer_segments as (
with purchase_stats as (
  SELECT
      a.mapped_user_id, 
      ex.app_type,
      ex._date, 
      min(date) AS first_purchase_date, 
      max(date) AS last_purchase_date,
      coalesce(sum(gms_net),0) AS lifetime_gms,
      coalesce(count(DISTINCT date),0) AS lifetime_purchase_days, 
      coalesce(count(DISTINCT receipt_id),0) AS lifetime_orders,
      round(cast(round(coalesce(sum(CASE
          WHEN date between date_sub(_date, interval 365 DAY) and _date THEN gms_net
      END), CAST(0 as NUMERIC)),20) as numeric),2) AS past_year_gms,
      count(DISTINCT CASE
          WHEN date between date_sub(_date, interval 365 DAY) and _date THEN date
      END) AS past_year_purchase_days,
      count(DISTINCT CASE
          WHEN date between date_sub(_date, interval 365 DAY) and _date THEN receipt_id
      END) AS past_year_orders
    from 
      `etsy-data-warehouse-prod.user_mart.mapped_user_profile` a
    join
       first_app_visits ex 
        ON ex.mapped_user_id = a.mapped_user_id
    join 
      `etsy-data-warehouse-prod.user_mart.user_mapping` b
        on a.mapped_user_id = b.mapped_user_id
    join 
      `etsy-data-warehouse-prod.user_mart.user_first_visits` c
        on b.user_id = c.user_id
    left join 
      `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` e
        on a.mapped_user_id = e.mapped_user_id 
        and e.date <= ex._date-1 and market <> 'ipp'
    GROUP BY all
    having (ex._date >= min(date(timestamp_seconds(a.join_date))) or ex._date >= min(date(c.start_datetime)))
  )
  select
    mapped_user_id, 
    app_type,
    _date,
    CASE  
      when mapped_user_id = 0 or mapped_user_id is null then 'Signed Out'
      when p.lifetime_purchase_days = 0 or p.lifetime_purchase_days is null then 'Zero Time'  
      when date_diff(_date,p.first_purchase_date, DAY)<=180 and (p.lifetime_purchase_days=2 or round(cast(round(p.lifetime_gms,20) as numeric),2) >100.00) then 'High Potential' 
      WHEN p.lifetime_purchase_days = 1 and date_diff(_date,p.first_purchase_date, DAY) <=365 then 'OTB'
      when p.past_year_purchase_days >= 6 and p.past_year_gms >=200 then 'Habitual' 
      when p.past_year_purchase_days>=2 then 'Repeat' 
      when date_diff(_date , p.last_purchase_date, DAY) >365 then 'Lapsed'
      else 'Active' 
      end as buyer_segment,
  from purchase_stats p
); 

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.rollups.first_app_visit` AS (
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
    min(case when app_type = 'esa' then visit_id end) as first_esa_visit_id, 
    min(case when app_type = 'esa' then buyer_segment end) as first_esa_buyer_segment,
  FROM first_app_visits
  left join buyer_segments using (mapped_user_id, _date, app_type)
  GROUP BY all
);
