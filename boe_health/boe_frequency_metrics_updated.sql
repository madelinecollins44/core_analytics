-- owner: kreitnauer@etsy.com
-- owner_team: product-asf-team@etsy.com
-- description: rollup for BOE DAU, WAU and MAU

BEGIN

--  insert most recent 7 days of data daily
--  include all of weblog.recent_visits in calculation tables so that we capture all of the possible browsers that visited in the past 30 days

--  distinct browsers & visit dates for the app
CREATE TEMPORARY TABLE browsers
  AS SELECT DISTINCT
      v.run_date,
      v.browser_id,
      v.platform,
      v.top_channel,
      v.region,
      v.browser_platform,
      vs.buyer_segment
    FROM
      `etsy-data-warehouse-prod.weblog.recent_visits` v
      LEFT JOIN `etsy-data-warehouse-prod.bot_mart.visit_bot_properties` p 
      on v.visit_id = p.visit_id and v._date = p._PARTITIONDATE
      left join etsy-data-warehouse-prod.rollups.visits_w_segments vs
      on v.visit_id = p.visit_id and v._date = p._date
    where v._date >= date_sub(current_date(), INTERVAL 60 DAY)
    and NOT (p.landing_page_type IN ('search_geonamessuggest_thrift', 'search_geonamessuggest_thrift_success')
        and p.backend_bot_checks.datacentre = true
        -- and p.platform = 'Android' 
        and p.header_fingerprint = 'uar' 
        and p.is_bounce = true
        and p.is_screenless = true
  --'filter reason is null' indicates that this visit was NOT filtered during bot filtering, and thus shows up in the visit log (this condition is not needed here because 
  -- we are joining with weblog.visits but leaving it here to align with the conditions mentioned in below doc and avoid any confusion
  --https://docs.google.com/document/d/1cDBiWaN6Teze1gM7uVQ8Z6jIepOKDEb6lV2OZ4EiuCs/edit
        and p.filter_reason IS NULL) 
  ;

--  table with each browser and the window of time over which it should be included as a WAU
--  a browser will be active from the day of the visit through the visit date plus 6 days (7 days total)
CREATE TEMPORARY TABLE wau_window
  AS  WITH status AS (
    SELECT
        browsers.browser_id,
        browsers.run_date,
        1 AS days_active_delta
      FROM
        browsers
    UNION ALL
    SELECT
        browsers_0.browser_id,
        browsers_0.run_date + 86400 * 6 AS run_date,
        -1 AS days_active_delta
      FROM
        browsers AS browsers_0
  ), rolling_states AS (
    SELECT
        a.browser_id,
        a.run_date,
        sum(a.days_active_delta) OVER (PARTITION BY a.browser_id ORDER BY a.run_date ROWS UNBOUNDED PRECEDING) > 0 AS active
      FROM
        (
          SELECT
              status.browser_id,
              status.run_date,
              sum(status.days_active_delta) AS days_active_delta
            FROM
              status
            GROUP BY 1, 2
        ) AS a
  ), deduplicated_states AS (
    SELECT
        b.browser_id,
        b.run_date,
        b.active
      FROM
        (
          SELECT
              rolling_states.browser_id,
              rolling_states.run_date,
              rolling_states.active,
              coalesce(rolling_states.active = lag(rolling_states.active, 1) OVER (PARTITION BY rolling_states.browser_id ORDER BY rolling_states.run_date), false) AS redundant
            FROM
              rolling_states
        ) AS b
      WHERE NOT b.redundant
  )
  SELECT
      c.browser_id,
      c.start_date,
      c.end_date
    FROM
      (
        SELECT
            deduplicated_states.browser_id,
            deduplicated_states.run_date AS start_date,
            coalesce(lead(deduplicated_states.run_date, 1) OVER (PARTITION BY deduplicated_states.browser_id ORDER BY deduplicated_states.run_date), UNIX_SECONDS(CAST(CAST(date_sub(current_date(), interval 1 DAY) as DATETIME) AS TIMESTAMP))) AS end_date,
            deduplicated_states.active
          FROM
            deduplicated_states
      ) AS c
    WHERE c.active
;

--  wau
--  one row for every day with the number of distinct browsers that visited in the prior 7 days
--  runs in about one minute
CREATE TEMPORARY TABLE wau
  AS SELECT
      DATE(cal.date) AS date,
      count(DISTINCT w.browser_id) AS wau
    FROM
      `etsy-data-warehouse-prod.public.calendar_dates` AS cal
      LEFT OUTER JOIN 
      (SELECT inner_cal.date as date, ww.browser_id
      FROM wau_window AS ww 
      JOIN `etsy-data-warehouse-prod.public.calendar_dates` AS inner_cal
      ON inner_cal.epoch_s BETWEEN ww.start_date AND ww.end_date) as w
      ON cal.date = w.date
    WHERE cal.date BETWEEN TIMESTAMP '2015-10-01 00:00:00' AND CAST(current_date() as TIMESTAMP)
    GROUP BY 1
;

--  table with each browser and the window of time over which it should be included as a MAU
--  a browser will be active from the day of the visit through the visit date plus 29 days (30 days total)
CREATE TEMPORARY TABLE mau_window
  AS  WITH status AS (
    SELECT
        browsers.browser_id,
        browsers.run_date,
        1 AS days_active_delta
      FROM
        browsers
    UNION ALL
    SELECT
        browsers_0.browser_id,
        browsers_0.run_date + 86400 * 29 AS run_date,
        -1 AS days_active_delta
      FROM
        browsers AS browsers_0
  ), rolling_states AS (
    SELECT
        a.browser_id,
        a.run_date,
        sum(a.days_active_delta) OVER (PARTITION BY a.browser_id ORDER BY a.run_date ROWS UNBOUNDED PRECEDING) > 0 AS active
      FROM
        (
          SELECT
              status.browser_id,
              status.run_date,
              sum(status.days_active_delta) AS days_active_delta
            FROM
              status
            GROUP BY 1, 2
        ) AS a
  ), deduplicated_states AS (
    SELECT
        b.browser_id,
        b.run_date,
        b.active
      FROM
        (
          SELECT
              rolling_states.browser_id,
              rolling_states.run_date,
              rolling_states.active,
              coalesce(rolling_states.active = lag(rolling_states.active, 1) OVER (PARTITION BY rolling_states.browser_id ORDER BY rolling_states.run_date), false) AS redundant
            FROM
              rolling_states
        ) AS b
      WHERE NOT b.redundant
  )
  SELECT
      c.browser_id,
      c.start_date,
      c.end_date
    FROM
      (
        SELECT
            deduplicated_states.browser_id,
            deduplicated_states.run_date AS start_date,
            coalesce(lead(deduplicated_states.run_date, 1) OVER (PARTITION BY deduplicated_states.browser_id ORDER BY deduplicated_states.run_date), UNIX_SECONDS(CAST(CAST(date_sub(current_date(), interval 1 DAY) as DATETIME) AS TIMESTAMP))) AS end_date,
            deduplicated_states.active
          FROM
            deduplicated_states
      ) AS c
    WHERE c.active
;

--  mau
--  a table with every day and the number of distinct browsers that visited in the prior 30 days
--  runs in about one minute
CREATE TEMPORARY TABLE mau
  AS SELECT
      DATE(cal.date) AS date,
      count(DISTINCT m.browser_id) AS mau
    FROM
      `etsy-data-warehouse-prod.public.calendar_dates` AS cal
      LEFT OUTER JOIN
      (SELECT inner_cal.date, mw.browser_id
      FROM mau_window AS mw
      JOIN `etsy-data-warehouse-prod.public.calendar_dates` AS inner_cal ON inner_cal.epoch_s BETWEEN mw.start_date AND mw.end_date) as m
      ON cal.date = m.date
    WHERE cal.date BETWEEN TIMESTAMP '2015-10-01 00:00:00' AND CAST(current_date() as TIMESTAMP)
    GROUP BY 1
;

--  dau, wau and mau on each day
CREATE TEMPORARY TABLE frequency_updates AS
   WITH dau AS (
    SELECT
        DATE(timestamp_seconds(browsers.run_date)) AS date,
        count(DISTINCT browsers.browser_id) AS dau
      FROM
        browsers
      WHERE browsers.run_date >= UNIX_SECONDS(CAST(CAST(DATE '2016-01-01' as DATETIME) AS TIMESTAMP))
      GROUP BY 1
    ORDER BY
      1
  ), yy AS (
    SELECT
        'ty' AS era,
        d.date,
        d.dau,
        w.wau,
        m.mau
      FROM
        dau AS d
        INNER JOIN wau AS w ON d.date = w.date
        INNER JOIN mau AS m ON d.date = m.date
    UNION ALL
    SELECT
        'ly' AS era,
        CAST(date_add(date, interval 52 WEEK) as DATETIME) AS date,
        dau_ty AS dau,
        wau_ty AS wau,
        mau_ty AS mau
      FROM
        `etsy-data-warehouse-dev.rollups.boe_frequency`
    ORDER BY
      1 NULLS LAST,
      2
  )
  SELECT
      DATE(yy.date) AS date,
      sum(CASE
        WHEN yy.era = 'ty' THEN yy.dau
      END) AS dau_ty,
      sum(CASE
        WHEN yy.era = 'ly' THEN yy.dau
      END) AS dau_ly,
      sum(CASE
        WHEN yy.era = 'ty' THEN yy.wau
      END) AS wau_ty,
      sum(CASE
        WHEN yy.era = 'ly' THEN yy.wau
      END) AS wau_ly,
      sum(CASE
        WHEN yy.era = 'ty' THEN yy.mau
      END) AS mau_ty,
      sum(CASE
        WHEN yy.era = 'ly' THEN yy.mau
      END) AS mau_ly
    FROM
      yy
    WHERE yy.date <= CAST(date_sub(current_date(), interval 1 DAY) as DATETIME)
    GROUP BY 1
  ORDER BY
    1
;

 --  replace the last 7 days of data
 DELETE FROM `etsy-data-warehouse-dev.rollups.boe_frequency` WHERE date >= date_sub(current_date(), interval 7 DAY);
 
 INSERT INTO `etsy-data-warehouse-dev.rollups.boe_frequency`
 SELECT * FROM frequency_updates WHERE date >= date_sub(current_date(), interval 7 DAY);

END;
-- --------------------------------------------------------------------

--  NOTE: this commented code was NOT converted from Vertica to BQ. Would require updating

--  code below here can be run if we need to recreate the base table
--  the final table will start with 01/01/2016
--  before running, add /*+direct*/ hints to the temp tables
--  couldn't comment the code with them in
-- --------------------------------------------------------------------
 /*
-- distinct browsers & visit dates for the app
drop table if exists browsers;
create temp table browsers on commit preserve rows as (
select 
run_date, 
browser_id,
max(app_name) as app_name,
max(canonical_region) as region
from weblog.visits
where app_name in ('ios-EtsyInc','android-EtsyInc')
group by 1,2
)
order by 2,1
;
-- table with each browser and the window of time over which it should be included as a WAU
-- a browser will be active from the day of the visit through the visit date plus 6 days (7 days total)
drop table if exists wau_window;
CREATE local temp TABLE wau_window on commit preserve rows AS (
WITH status AS (
    SELECT 
    browser_id
    ,run_date 
    --,app_name
    --,region
    ,1 AS days_active_delta
FROM browsers
UNION ALL
    SELECT 
    browser_id
    ,run_date + (86400*6) AS run_date
    --,app_name
    --,region
    ,-1 AS days_active_delta
FROM browsers
),
rolling_states AS (
SELECT 
browser_id
,run_date
--,app_name
--,region
,SUM(days_active_delta) OVER (PARTITION BY browser_id ORDER BY run_date ASC ROWS UNBOUNDED PRECEDING) > 0 AS active
    FROM (
        SELECT 
        browser_id
        ,run_date
        --,app_name
        --,region
        ,SUM(days_active_delta) AS days_active_delta
        FROM status
        GROUP BY 1,2--,3,4
    ) a
),
deduplicated_states AS (
SELECT 
browser_id
,run_date
--,app_name
--,region
,active
    FROM (
        SELECT 
        browser_id
        ,run_date
        --,app_name
        --,region
        ,active
        ,COALESCE(active = LAG(active, 1) OVER (PARTITION BY browser_id ORDER BY run_date ASC),FALSE) AS redundant
        FROM rolling_states
    ) b
WHERE NOT redundant
)
SELECT browser_id
--,app_name
--,region
,start_date
,end_date
    FROM (
        SELECT 
        browser_id
        --,app_name
        --,region
        ,run_date AS start_date
        ,COALESCE(LEAD(run_date, 1) OVER (PARTITION BY browser_id ORDER BY run_date ASC),extract(epoch from CURRENT_DATE - interval '1 day')) AS end_date
        ,active
        FROM deduplicated_states
    ) c
WHERE active
);
-- wau
-- table with every day and the number of distinct browsers that visited in the prior 7 days
-- runs in ten-ish minutes
drop table if exists wau;
create local temp table wau on commit preserve rows as (
SELECT 
date::date as date,
--app_name,
--region,
count(distinct browser_id) wau
FROM public.calendar_dates cal
LEFT JOIN wau_window AS w
    ON epoch_s BETWEEN w.start_date AND w.end_date
    and date between '20151001' and current_date     
where date between '20151001' and current_date
group by 1--,2,3
)
order by 1--,2,3
;
-- a table with each browser and the window of time over which it should be included as a MAU
-- a browser will be active from the day of the visit through the visit date plus 29 days (30 days total)
drop table if exists mau_window;
CREATE local temp TABLE mau_window on commit preserve rows AS (
WITH status AS (
    SELECT 
    browser_id
    ,run_date 
    ,app_name
    ,region
    ,1 AS days_active_delta
FROM browsers
UNION ALL
    SELECT 
    browser_id
    ,run_date + (86400*29) AS run_date
    ,app_name
    ,region
    ,-1 AS days_active_delta
FROM browsers
),
rolling_states AS (
SELECT 
browser_id
,run_date
,app_name
,region
,SUM(days_active_delta) OVER (PARTITION BY browser_id ORDER BY run_date ASC ROWS UNBOUNDED PRECEDING) > 0 AS active
    FROM (
        SELECT 
        browser_id
        ,run_date
        ,app_name
        ,region
        ,SUM(days_active_delta) AS days_active_delta
        FROM status
        GROUP BY 1,2,3,4
    ) a
),
deduplicated_states AS (
SELECT 
browser_id
,run_date
,app_name
,region
,active
    FROM (
        SELECT 
        browser_id
        ,run_date
        ,app_name
        ,region
        ,active
        ,COALESCE(active = LAG(active, 1) OVER (PARTITION BY browser_id ORDER BY run_date ASC),FALSE) AS redundant
        FROM rolling_states
    ) b
WHERE NOT redundant
)
SELECT browser_id
,app_name
,region
,start_date
,end_date
    FROM (
        SELECT 
        browser_id
        ,app_name
        ,region
        ,run_date AS start_date
        ,COALESCE(LEAD(run_date, 1) OVER (PARTITION BY browser_id ORDER BY run_date ASC),extract(epoch from CURRENT_DATE - interval '1 day')) AS end_date
        ,active
        FROM deduplicated_states
    ) c
WHERE active
);
-- mau
-- a table with every day and the number of distinct browsers that visited in the prior 30 days
-- runs in ten-ish minutes
drop table if exists mau;
create local temp table mau on commit preserve rows as (
SELECT 
date::date as date,
count(distinct browser_id) mau
FROM public.calendar_dates cal
LEFT JOIN mau_window AS m
    ON epoch_s BETWEEN m.start_date AND m.end_date
    and date between '20151001' and current_date     
where date between '20151001' and current_date
group by 1--,2,3
)
order by 1--,2,3
;
-- final table with dau, wau and mau on each day
drop table if exists rollups.boe_frequency;
create table rollups.boe_frequency (
date date not null,
dau_ty int,
dau_ly int,
wau_ty int,
wau_ly int,
mau_ty int,
mau_ly int
)
order by date
segmented by hash(date) all nodes ksafe 1
partition by date
;
insert into rollups.boe_frequency (
with dau as (
select
to_timestamp(run_date)::date as date,
count(distinct browser_id) as dau
from browsers
where run_date >= extract(epoch from date('20160101'))
group by 1
order by 1
),
yy as (
select 
'ty' as era,
d.date,
dau,
wau,
mau
from dau d
    inner join wau w
    on d.date = w.date
    inner join mau m
    on d.date = m.date
union all
select
'ly' as era,
d.date + interval '52 weeks' as date,
dau,
wau,
mau
from dau d
    inner join wau w
    on d.date = w.date
    inner join mau m
    on d.date = m.date
order by 1,2
)
select
date::date as date,
sum(case when era = 'ty' then dau end) dau_ty,
sum(case when era = 'ly' then dau end) dau_ly,
sum(case when era = 'ty' then wau end) wau_ty,
sum(case when era = 'ly' then wau end) wau_ly,
sum(case when era = 'ty' then mau end) mau_ty,
sum(case when era = 'ly' then mau end) mau_ly
from yy
where date <= current_date - interval '1 day'
group by 1
order by 1
)
;
grant select on rollups.boe_frequency to analysts_role, data_eng_role, finance_accounting_role, ads_role, pattern_role, payments_role, shipping_role, kpi_role;
 */
