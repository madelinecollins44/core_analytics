--redid table to include 
select * from etsy-data-warehouse-dev.rollups.boe_user_retention_yoy where ty_browsers_with_first_visit is not null and ty_browsers_visit_in_first_14_days >0 
--678931407, 1 visit in last 14 days
--954602148, 1 visit in last 7 days 
--48422026, 2 visits w first visit 


-- check indivual mapped_user_ids
  select
  v.browser_id,
  v.browser_platform,
  v.region,
  v._date as first_app_visit,
  v.user_id,
  v.event_source,
  v.start_datetime,
  u.mapped_user_id,
  case when v.user_id is not null then 1 else 0 end as is_signed_in,
  lead(v._date) over (partition by v.browser_id order by v.start_datetime asc) as next_visit_date
from 
  `etsy-data-warehouse-prod.weblog.visits` v  
left join `etsy-data-warehouse-prod.user_mart.user_mapping` u 
  on v.user_id = u.user_id
where 
  v.platform = "boe"
  -- and v._date is not null 
  and v._date >= current_date-365
  and v.event_source in ("ios", "android")
  and v.platform in ('boe')
  and (mapped_user_id = 678931407 or mapped_user_id = 954602148 or mapped_user_id = 48422026)
group by all
qualify row_number() over(partition by v.browser_id order by start_datetime) = 1
-- browser_id	browser_platform	region	first_app_visit	user_id	event_source	start_datetime	mapped_user_id	is_signed_in	next_visit_date
-- 38720DA78FEF46F79877B05373B9	iOS	GB	2024-07-23	954602446	ios	2024-07-23 12:41:08.000000 UTC	954602148	1	2024-07-23

-- 7866361F76F047CFB9C25ADF1979	iOS	US	2024-05-31	678931407	ios	2024-05-31 14:51:49.000000 UTC	678931407	1	2024-06-07
-- 1FFBEB2249154934950473189D12	iOS	US	2023-11-03	678931407	ios	2023-11-03 20:11:44.000000 UTC	678931407	1	2023-11-05

-- m5mV1hqmTVCSb69Ky3xLrg	Android	US	2024-02-22	48422026	android	2024-02-22 16:30:38.000000 UTC	48422026	1	2024-02-23
-- MoVDSd6HRbCaGTavR_65PA	Android	US	2024-02-22	48422026	android	2024-02-22 20:31:24.000000 UTC	48422026	1	2024-02-24
