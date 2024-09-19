-----QUESTION TO ANSWER: Which channels drive visits from reactivated app users? How long have they been inactive before reactivating?
-- base table
with reactivated_boe_visits as (
  select
  _date
  , visit_id
  , mapped_user_id
  , case 
      when top_channel in ('direct') then 'Direct'
      when top_channel in ('dark') then 'Dark'
      when top_channel in ('internal') then 'Internal'
      when top_channel in ('seo') then 'SEO'
      when top_channel like 'social_%' then 'Non-Paid Social'
      when top_channel like 'email%' then 'Email'
      when top_channel like 'push_%' then 'Push'
      when top_channel in ('us_paid','intl_paid') then
        case
          when (second_channel like '%gpla' or second_channel like '%bing_plas') then 'PLA'
          when (second_channel like '%_ppc' or second_channel like 'admarketplace') then case
          when third_channel like '%_brand' then 'SEM - Brand' else 'SEM - Non-Brand'
          end
      when second_channel='affiliates' then 'Affiliates'
      when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
      when second_channel like '%native_display' then 'Display'
      when second_channel in ('us_video','intl_video') then 'Video' else 'Other Paid' end
      else 'Other Non-Paid' 
      end as reporting_channel
    , lag(_date) over(partition by mapped_user_id order by _date) as previous_boe_visit_date
    , lag(start_datetime) over(partition by mapped_user_id order by start_datetime) as previous_boe_visit_start_datetime
    , date_diff(_date, lag(_date) over (partition by mapped_user_id order by _date asc), day) AS days_between_visits
from 
  etsy-data-warehouse-prod.weblog.visits 
left join 
  etsy-data-warehouse-prod.user_mart.user_mapping
    using (user_id)
where 
    date_trunc(_date, year) >= '2022-01-01'
    and platform in ('boe')
  qualify timestamp_diff(start_datetime, lag(start_datetime) over(partition by mapped_user_id order by start_datetime), minute) >= 60 -- only pulling visits that happened 60+ minutes apart
--qualify date_diff(_date, lag(_date) over (partition by mapped_user_id order by _date), day) >= 30 -- remove this for all boe users 
  )

 -- day metrics  
select
  reporting_channel
  , count(distinct case when days_between_visits >= 30 then mapped_user_id end) as total_reactivated_users_after_30_days
  , max(days_between_visits) as max_days_between_visits
  , min(days_between_visits) as min_days_between_visits
  , avg(days_between_visits) as avg_days_between_visits
  , approx_quantiles(days_between_visits, 100)[offset(50)] as median_days_between_visits
from reactivated_boe_visits
group by all 

-- medians 
with medians as (select
  reporting_channel,
  days_between_visits,
  percentile_cont(days_between_visits, 0.5) over (partition by reporting_channel) as approx_median
from etsy-bigquery-adhoc-prod._script17836779648db16bb26cbdc4066d92fd64c1153b.boe_visits
group by all 
)
select distinct reporting_channel, approx_median from medians group by all order by 1 desc

-----engagement metrics
select
  reporting_channel
  , count(distinct case when days_between_visits >= 30 then mapped_user_id end) as total_reactivated_users_after_30_days
  , count(distinct case when days_between_visits >= 30 then visit_id end) as total_visits_from_reactivated_users_after_30_days
  , count(distinct visit_id) as total_visits
  , max(days_between_visits) as max_days_between_visits
  , min(days_between_visits) as min_days_between_visits
  , avg(days_between_visits) as avg_days_between_visits
  , approx_quantiles(days_between_visits, 100)[offset(50)] as median_days_between_visits
from
etsy-bigquery-adhoc-prod._script588150b750909492c719faa564cc809f9ad8100d.boe_visits -- temp table w all visits since 1/1/2022
 group by all 

-----QUESTION TO ANSWER: days since visit
select
  reporting_channel
  , case 
    when days_between_visits = 30 then '1 month'
    when days_between_visits between 31 and 60 then '2 months'
    when days_between_visits between 61 and 90 then '3 months'
    when days_between_visits between 91 and 120 then '4 months'
    when days_between_visits between 121 and 150 then '5 months'
    when days_between_visits between 151 and 180 then '6 months'
    when days_between_visits between 181 and 210 then '7 months'
    when days_between_visits between 211 and 240 then '8 months'
    when days_between_visits between 241 and 270 then '9 months'
    when days_between_visits between 271 and 300 then '10 months'
    when days_between_visits between 301 and 330 then '11 months'
    when days_between_visits between 331 and 365 then '1 year'
    when days_between_visits between 366 and 548 then '1.5 years'
    when days_between_visits between 549 and 730 then '2 year'
    else 'more than 2 years'
    end as days_between_visits    
  , count(distinct mapped_user_id) as users
from reactivated_boe_visits
group by all 
  
--different way of doing day groups
, date_groups as (
select
  days_between_visits
  , floor((days_between_visits/30)) as monthly_group
from reactivated_boe_visits
group by all 
)
select 
  reporting_channel
  , monthly_group
  , count(distinct mapped_user_id) as users
  , count(distinct visit_id) as visits
from reactivated_boe_visits
left join date_groups using (days_between_visits)
group by all 


---find share of total traffic vs share of traffic from reactivated users 
  select reporting_channel, count(distinct visit_id) as visits, count(distinct mapped_user_id) as users 
  from 
  --etsy-bigquery-adhoc-prod._script6b45a761fa55bcc614e89e599787433662a5642e.reactivated_boe_visits
  etsy-bigquery-adhoc-prod._script588150b750909492c719faa564cc809f9ad8100d.boe_visits
  group by all
  
------------------------------------------------------------------------------------------------------------------------------------------------
--TESTING
------------------------------------------------------------------------------------------------------------------------------------------------
--identify users with 365 days before a next visit + testing this first step 
with agg as (
  select
  _date
  , visit_id
  , mapped_user_id
  , lag(_date) over (partition by mapped_user_id order by _date asc) as last_visit_date
  , date_diff(_date, lag(_date) over (partition by mapped_user_id order by _date asc), day) AS days_between_visits
from 
  etsy-data-warehouse-prod.weblog.visits 
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile 
    using (user_id)
where 
  _date >= current_date-15)
select * from agg where days_between_visits >= 8
  -- select * from agg where days_between_visits >= 3 and last_visit_date = '2024-09-01'

-- _date	visit_id	mapped_user_id	last_visit_date	days_between_visits
-- 2024-09-03	d2ffc9432f20431abddae42b9752.1725386242345.1	131198157	2024-08-26	8
-- 2024-09-03	PtxwNcn7mJhHZzYwqplsp9raNNzX.1725372252273.1	298808605	2024-08-26	8
-- 2024-09-03	ukxy7YUHKO9X6nqclIqA-KdAUHy_.1725325076401.2	619366859	2024-08-26	8
-- 2024-09-04	6a32a6f0e89445b9abf7cf80766c.1725487734323.1	313322560	2024-09-01	3
-- 2024-09-04	258565B1BB6F4B3383BC7F427C0C.1725468544318.1	792003065	2024-09-01	3

--test w weblog.visits to make sure it works 
select
  _date
  , mapped_user_id
from etsy-data-warehouse-prod.weblog.visits 
left join etsy-data-warehouse-prod.user_mart.mapped_user_profile using (user_id)
where (mapped_user_id = 313322560 or mapped_user_id= 131198157 or mapped_user_id=619366859)
and _date >= current_date-15 order by _date desc
-- _date	mapped_user_id
-- 2024-09-09	131198157
-- 2024-09-08	131198157
-- 2024-09-04	131198157
-- 2024-09-03	131198157
-- 2024-09-03	131198157
-- 2024-08-26	131198157
-- 2024-09-07	313322560
-- 2024-09-06	313322560
-- 2024-09-05	313322560
-- 2024-09-05	313322560
-- 2024-09-05	313322560
-- 2024-09-05	313322560
-- 2024-09-04	313322560
-- 2024-09-01	313322560
-- 2024-09-01	313322560
-- 2024-08-30	313322560
-- 2024-08-28	313322560
-- 2024-09-03	619366859
-- 2024-08-26	619366859

--all visits 
select 
   case 
      when top_channel in ('direct') then 'Direct'
      when top_channel in ('dark') then 'Dark'
      when top_channel in ('internal') then 'Internal'
      when top_channel in ('seo') then 'SEO'
      when top_channel like 'social_%' then 'Non-Paid Social'
      when top_channel like 'email%' then 'Email'
      when top_channel like 'push_%' then 'Push'
      when top_channel in ('us_paid','intl_paid') then
        case
          when (second_channel like '%gpla' or second_channel like '%bing_plas') then 'PLA'
          when (second_channel like '%_ppc' or second_channel like 'admarketplace') then case
          when third_channel like '%_brand' then 'SEM - Brand' else 'SEM - Non-Brand'
          end
      when second_channel='affiliates' then 'Affiliates'
      when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
      when second_channel like '%native_display' then 'Display'
      when second_channel in ('us_video','intl_video') then 'Video' else 'Other Paid' end
      else 'Other Non-Paid' 
      end as reporting_channel
  , count(distinct visit_id) as visits 
from 
  etsy-data-warehouse-prod.weblog.visits 
where 
    date_trunc(_date, year) >= '2022-01-01'
group by all 


--testing to see why there are so many visits with 0 days between visits
select 
  visit_id 
  -- mapped_user_id
  from boe_visits where days_between_visits = 0

-- visit_id
-- cX-mTuu0Tj6JVrN7tWAHMg.1718095800152.1
-- 8D04A31AD670490B9FF24F0850CB.1718066167031.2
-- 2BA642BE93C34549BBD510912D0C.1708186353369.2
-- ohAime_ZQqWAP_oDnREi0w.1721140892189.1
-- 0VbZ_tptxGfgwo8B-lVLJ1On3Asa.1713030752088.1
-- Dfi2gKBbR5iFx4sStYMx-A.1710558024503.2
-- 4E49E27A34EF4345BBB00782CCE9.1715011602776.2
-- 25C4EBE8E703419CA01A4F2BF447.1718212055443.1
-- Pv3oWrElQfG4LR1EDRk96w.1723397886682.1

-- mapped_user_id
-- 12661
-- 51505
-- 66467
-- 85954
-- 101066
-- 101774

select 
  platform, top_channel, visit_id, _date, start_datetime 
from 
  etsy-data-warehouse-prod.weblog.visits a
left join 
  etsy-data-warehouse-prod.user_mart.user_mapping b
    using (user_id)
where 
    date_trunc(_date, year) >= '2024-01-01'
    and platform in ('boe')
    and mapped_user_id = 12661
order by _date, start_datetime desc)
----------bascially, see instances where a user visited multiple times in same day --> will add in start_datetime to remove any instances where next visit is within the hour OR remove instances of users that have a difference of a day 
-- platform	top_channel	visit_id	_date	start_datetime
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1705546618085.13	2024-01-18	2024-01-18 02:56:58.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1705546540702.12	2024-01-18	2024-01-18 02:55:40.000000 UTC
-- boe	other_utms	31AB0903B8D04D1C9721EA5FBCDC.1705544599120.11	2024-01-18	2024-01-18 02:23:19.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1705544517039.10	2024-01-18	2024-01-18 02:21:57.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1705544509904.9	2024-01-18	2024-01-18 02:21:49.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1705544483459.8	2024-01-18	2024-01-18 02:21:23.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1705544432068.7	2024-01-18	2024-01-18 02:20:32.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1705544407012.6	2024-01-18	2024-01-18 02:20:07.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1705544366864.5	2024-01-18	2024-01-18 02:19:26.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1705544351050.4	2024-01-18	2024-01-18 02:19:11.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1705544339846.3	2024-01-18	2024-01-18 02:18:59.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1705543250354.2	2024-01-18	2024-01-18 02:00:50.000000 UTC
-- boe	other_utms	31AB0903B8D04D1C9721EA5FBCDC.1705543168307.1	2024-01-18	2024-01-18 01:59:28.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1706370753047.1	2024-01-27	2024-01-27 15:52:33.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1706467567977.1	2024-01-28	2024-01-28 18:46:07.000000 UTC
-- boe	seo	31AB0903B8D04D1C9721EA5FBCDC.1706659305992.1	2024-01-31	2024-01-31 00:01:45.000000 UTC
-- boe	other_utms	31AB0903B8D04D1C9721EA5FBCDC.1707244572344.2	2024-02-06	2024-02-06 18:36:12.000000 UTC
-- boe	other_referrer_wo_utms	31AB0903B8D04D1C9721EA5FBCDC.1707244539104.1	2024-02-06	2024-02-06 18:35:39.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1708171368123.2	2024-02-17	2024-02-17 12:02:48.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1708171363620.1	2024-02-17	2024-02-17 12:02:43.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1708956364739.2	2024-02-26	2024-02-26 14:06:04.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1708956288317.1	2024-02-26	2024-02-26 14:04:48.000000 UTC
-- boe	push_adhoc	31AB0903B8D04D1C9721EA5FBCDC.1709832550977.1	2024-03-07	2024-03-07 17:29:10.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1710028624336.1	2024-03-09	2024-03-09 23:57:04.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1715706577381.1	2024-05-14	2024-05-14 17:09:37.000000 UTC
-- boe	us_paid	31AB0903B8D04D1C9721EA5FBCDC.1716485632418.1	2024-05-23	2024-05-23 17:33:52.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1720229504000.1	2024-07-06	2024-07-06 01:31:44.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1721010311423.1	2024-07-15	2024-07-15 02:25:11.000000 UTC
-- boe	push_trans	31AB0903B8D04D1C9721EA5FBCDC.1721302354034.2	2024-07-18	2024-07-18 11:32:34.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1721302272244.1	2024-07-18	2024-07-18 11:31:12.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1723285628161.1	2024-08-10	2024-08-10 10:27:08.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1724120770771.1	2024-08-20	2024-08-20 02:26:10.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1724247424810.1	2024-08-21	2024-08-21 13:37:04.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1724292398897.1	2024-08-22	2024-08-22 02:06:38.000000 UTC
-- boe	push_lifecycle	31AB0903B8D04D1C9721EA5FBCDC.1725964217929.2	2024-09-10	2024-09-10 10:30:17.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1725964160571.1	2024-09-10	2024-09-10 10:29:20.000000 UTC
-- boe	push_adhoc	31AB0903B8D04D1C9721EA5FBCDC.1725932270741.2	2024-09-10	2024-09-10 01:37:50.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1725932230620.1	2024-09-10	2024-09-10 01:37:10.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1726067200814.1	2024-09-11	2024-09-11 15:06:40.000000 UTC
-- boe	direct	31AB0903B8D04D1C9721EA5FBCDC.1726324442631.1	2024-09-14	2024-09-14 14:34:02.000000 UTC
