-- this rollup looks at all visits in time frame, and of those users that have visited, when is their most recent visit on each platform
BEGIN

declare last_date date;

drop table if exists `etsy-data-warehouse-dev.rollups.boe_churn_segmentation`;

-- create table if not exists `etsy-data-warehouse-dev.rollups.boe_churn_segmentation`  (
create table if not exists `etsy-data-warehouse-dev.rollups.boe_churn_segmentation`  (
 _date DATE
  -- , region STRING
  -- , admin INT64
  -- , buyer_segment STRING
  , days_since_boe_visit INT64
  , days_since_mweb_visit INT64
  , days_since_desktop_visit INT64
  , unique_users INT64
); 

-- set last_date = (select max(_date) from `etsy-data-warehouse-dev.rollups.boe_churn_segmentation`);
--  if last_date is null then set last_date = (select min(_date)-1 from `etsy-data-warehouse-prod.weblog.events`);
--  end if;

set last_date = current_date-10; 

--start with all visit data 
create or replace temporary table all_data as (
with agg_visit as (
select
 a.user_id
  , a.platform 
  , a.browser_platform
	-- , a.region  
  -- , a.is_admin_visit as admin
  -- , a.top_channel 
  , a.visit_id
  , a._date
  , a.start_datetime
  , unix_seconds(timestamp (a.start_datetime)) as start_time
  , row_number() over (partition by a.user_id order by unix_seconds(timestamp (a.start_datetime)) desc, a.visit_id desc) AS visit_order
  -- , b.buyer_segment
from 
  etsy-data-warehouse-prod.weblog.visits a 
left join 
  mapped_user_profile b using (user_id)
where _date >= current_date-3
)
, last_visit_platform as (
select 
  user_id
  -- , region
  -- , admin
  , platform
  -- , buyer_segment
  , min(visit_order) as most_recent_visit
  , cast(max(_date) as date) as most_recent_visit_date
from agg_visit
group by all 
)
select
    user_id
  -- , region
  -- , admin
  -- , buyer_segment
  , current_date as _date
  , coalesce(max(case when platform in ('boe') then date_diff(current_date, most_recent_visit_date, day) else null end),0) as days_since_boe_visit
  , coalesce(max(case when platform in ('mobile_web') then date_diff(current_date, most_recent_visit_date, day) else null end),0) as days_since_mweb_visit
  , coalesce(max(case when platform in ('desktop') then date_diff(current_date, most_recent_visit_date, day)else null end),0) as days_since_desktop_visit
from 
  last_visit_platform
group by all 
);

insert into `etsy-data-warehouse-dev.rollups.boe_churn_segmentation` (
select
  _date
  -- , region
  -- , admin
  -- , buyer_segment
  , days_since_boe_visit
  , days_since_mweb_visit
  , days_since_desktop_visit
  , count(distinct user_id) as unique_users
from all_data
group by all 
); 

end
