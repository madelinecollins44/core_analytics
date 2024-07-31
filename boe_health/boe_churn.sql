-----this rollup calcs how long it has been since a users last visit on each platform as of today. the segmentations in this table are the most recent visit segmentations.
BEGIN

declare last_date date;

-- drop table if exists `etsy-data-warehouse-dev.rollups.boe_churn_segmentation`;

-- create table if not exists `etsy-data-warehouse-dev.rollups.boe_churn_segmentation`  (
--   _date DATE
--   , most_recent_buyer_segment STRING
--   , most_recent_region STRING
--   , most_recent_signed_in STRING
--   , most_recent_browser_platform STRING
--   , days_since_boe_visit STRING
--   , days_since_mweb_visit STRING
--   , user_count int64
-- ); 

-- set last_date = (select max(_date) from `etsy-data-warehouse-dev.rollups.boe_churn_segmentation`);
--  if last_date is null then set last_date = (select min(_date)-1 from `etsy-data-warehouse-prod.weblog.events`);
--  end if;

set last_date = current_date-365; 

create or replace temp table combine_all_platforms as (
with all_visits as (
select
 a.user_id
  , a.platform 
  , case when a.user_id is not null then 1 else 0 end as signed_in
  , a.browser_platform
	, a.region  
  , b.buyer_segment
  , a._date
  , a.start_datetime
  , unix_seconds(timestamp (a.start_datetime)) as start_time
  , row_number() over (partition by user_id, platform order by unix_seconds(timestamp (a.start_datetime)) desc) as rn -- gives order of visit by platform
from 
  etsy-data-warehouse-prod.weblog.visits a 
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile b using (user_id)
where _date >= last_date
)
-- this pulls the most recent visit on boe for each user 
, last_boe_visit as (
select * from all_visits where platform in ('boe') and rn=1
)
-- this pulls the most recent visit on mobile_web and desktop for each user 
, last_mweb_desktop_visit_raw as (
select 
  *,  
  row_number() over (partition by user_id order by start_time desc) as rn_clean -- need this step to order visits between mweb + desktop, then filtering in next step to ensure user_id is only used once
from all_visits where platform in ('mobile_web','desktop') and rn=1
)
-- this pulls the most recent visit on mobile_web or desktop for each user 
, last_mweb_desktop_visit as (
select * from last_mweb_desktop_visit_raw where rn_clean =1
)
-- , combine_all_platforms as (
select 
   user_id
  , 'boe' as platform 
  , signed_in
  , browser_platform
	, region  
  , buyer_segment
  , _date as visit_date
  , start_time
from last_boe_visit
union all 
select
  user_id
  , 'mweb/ desktop' as platform 
  , signed_in
  , browser_platform
	, region  
  , buyer_segment
  , _date as visit_date
  , start_time
from last_mweb_desktop_visit
);

create or replace temp table most_recent_visit as (
select 
  user_id
  , buyer_segment 
  , region
  , signed_in
  , browser_platform 
from combine_all_platforms 
qualify row_number() over(partition by user_id order by start_time) = 1
);

--this is the combined table that takes the most recent visit information for each user, and then calcs the number of days between each visit 
create or replace table etsy-data-warehouse-dev.rollups.boe_churn_segmentation as (
select
  current_date as _date
  , b.buyer_segment as most_recent_buyer_segment
  , b.region as most_recent_region
  , b.signed_in as most_recent_signed_in
  , b.browser_platform as most_recent_browser_platform
  , coalesce(max(case when a.platform in ('boe') then date_diff(current_date, a.visit_date, day) else null end),0) as days_since_boe_visit
  , coalesce(max(case when a.platform in ('mweb/ desktop') then date_diff(current_date, a.visit_date, day) else null end),0) as days_since_mweb_visit
  , count(distinct user_id) as user_count
from 
  combine_all_platforms a
left join 
  most_recent_visit b
    using (user_id)
  group by all 
);

END 


-- ------testing
-- select * from last_mweb_desktop_visit where user_id = 928050874

-- -- no boe visit for 928050874 -- desktop only 
-- -- select user_id, count(*) from all_visits group by all order by 2 desc


-- ---tested w single user_-d and confirmed everything workds as it should 
-- -- , final as (
-- select
--   current_date as _date
--   , user_id
--   , b.buyer_segment as most_recent_buyer_segment
--   , b.region as most_recent_region
--   , b.signed_in as most_recent_signed_in_status
--   , b.browser_platform as most_recent_browser_platform
--   , coalesce(max(case when a.platform in ('boe') then date_diff(current_date, a.visit_date, day) else null end),0) as days_since_boe_visit
--   , coalesce(max(case when a.platform in ('mweb/ desktop') then date_diff(current_date, a.visit_date, day) else null end),0) as days_since_mweb_visit
--   , count(distinct user_id) as user_count
-- from 
--   combine_all_platforms a
-- left join 
--   most_recent_visit b
--     using (user_id)
-- where user_id = 691894907
--   group by all 
