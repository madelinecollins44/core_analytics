-----QUESTION TO ANSWER: Which channels drive visits from reactivated app users? How long have they been inactive before reactivating?
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
  , lag(_date) over (partition by mapped_user_id order by _date asc) as last_visit_date
  , date_diff(_date, lag(_date) over (partition by mapped_user_id order by _date asc), day) AS days_between_visits
from 
  etsy-data-warehouse-prod.weblog.visits 
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile 
    using (user_id)
where 
    date_trunc(_date, year) >= '2022-01-01'
    and platform in ('boe')
qualify date_diff(_date, lag(_date) over (partition by mapped_user_id order by _date asc), day) >= 30)
select
  reporting_channel
  , count(distinct case when days_between_visits >= 30 then mapped_user_id end) as total_reactivated_users_after_30_days
  , max(days_between_visits) as max_days_between_visits
  , min(days_between_visits) as min_days_between_visits
  , avg(days_between_visits) as avg_days_between_visits
  , approx_quantiles(days_between_visits, 100)[offset(50)] as median_days_between_visits
from reactivated_boe_visits
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