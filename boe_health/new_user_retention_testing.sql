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




--- test mapper_user_ids
with yoy_union as (
select 
  'ty' as era,
  a.first_app_visit,
  browser_platform,
  region,
  buyer_segment,
  a.mapped_user_id,
  --users
  count(distinct a.mapped_user_id) as users_with_first_visit,
  count(distinct case when a.first_app_visit = next_visit_date then a.mapped_user_id end) as users_visit_in_same_day,
  count(distinct case when next_visit_date <= a.first_app_visit + 6 then a.mapped_user_id end) as users_visit_in_first_7_days,
  count(distinct case when next_visit_date <= a.first_app_visit + 13 then a.mapped_user_id end) as users_visit_in_first_14_days,
  count(distinct case when next_visit_date <= a.first_app_visit + 29 then a.mapped_user_id end) as users_visit_in_first_30_days,
from 
  etsy-bigquery-adhoc-prod._scriptd26af5f9b3fcea653e8d093c64bd2866eef58a64.first_visits_users a
left join 
  etsy-bigquery-adhoc-prod._script541933f33cf3f171ab378ef1568beb41bfbe0d00.buyer_segments
 b 
    on a.mapped_user_id=b.mapped_user_id
    and a.first_app_visit=b.first_app_visit
group by all
union all
select
  'ly' as era,
  CAST(date_add(a.first_app_visit, interval 52 WEEK) as DATETIME) AS first_app_visit,
  browser_platform,
  region,
  buyer_segment,--segment when they downloaded the app
  a.mapped_user_id,
  --users
  count(distinct a.mapped_user_id) as users_with_first_visit,
  count(distinct case when a.first_app_visit = next_visit_date then a.mapped_user_id end) as users_visit_in_same_day,
  count(distinct case when next_visit_date <= a.first_app_visit + 6 then a.mapped_user_id end) as users_visit_in_first_7_days,
  count(distinct case when next_visit_date <= a.first_app_visit + 13 then a.mapped_user_id end) as users_visit_in_first_14_days,
  count(distinct case when next_visit_date <= a.first_app_visit + 29 then a.mapped_user_id end) as users_visit_in_first_30_days,
from 
  etsy-bigquery-adhoc-prod._scriptd26af5f9b3fcea653e8d093c64bd2866eef58a64.first_visits_users a
left join 
  etsy-bigquery-adhoc-prod._script541933f33cf3f171ab378ef1568beb41bfbe0d00.buyer_segments
 b 
    on a.mapped_user_id=b.mapped_user_id
    and a.first_app_visit=b.first_app_visit
group by all 
)
SELECT
  cast(first_app_visit as date) as first_app_visit,
  browser_platform,
  region,
  buyer_segment,
  mapped_user_id,
  --ty metrics
  sum(CASE WHEN era = 'ty' THEN users_with_first_visit END) AS ty_users_with_first_visit,
  sum(CASE WHEN era = 'ty' THEN users_visit_in_same_day END) AS ty_users_visit_in_same_day,
  sum(CASE WHEN era = 'ty' THEN users_visit_in_first_7_days END) AS ty_users_visit_in_first_7_days,
  sum(CASE WHEN era = 'ty' THEN users_visit_in_first_14_days END) AS ty_users_visit_in_first_14_days,
  sum(CASE WHEN era = 'ty' THEN users_visit_in_first_30_days END) AS ty_users_visit_in_first_30_days,
  --ly metrics
  sum(CASE WHEN era = 'ly' THEN users_with_first_visit END) AS ly_users_with_first_visit,
  sum(CASE WHEN era = 'ly' THEN users_visit_in_first_7_days END) AS ly_users_visit_in_first_7_days,
  sum(CASE WHEN era = 'ly' THEN users_visit_in_first_14_days END) AS ly_users_visit_in_first_14_days,
  sum(CASE WHEN era = 'ly' THEN users_visit_in_first_30_days END) AS ly_users_visit_in_first_30_days,
  FROM
    yoy_union
  WHERE first_app_visit < CAST(current_date() as DATETIME)
  and (mapped_user_id = 678931407 or mapped_user_id = 954602148 or mapped_user_id =48422026)
  GROUP BY all
-- first_app_visit	browser_platform	region	buyer_segment	mapped_user_id	ty_users_with_first_visit	ty_users_visit_in_same_day	ty_users_visit_in_first_7_days	ty_users_visit_in_first_14_days	ty_users_visit_in_first_30_days	ly_users_with_first_visit	ly_users_visit_in_first_7_days	ly_users_visit_in_first_14_days	ly_users_visit_in_first_30_days
-- 2024-07-06	iOS	US	Active	678931407	1	0	0	1	1				
-- 2024-07-23	iOS	GB	Zero Time	954602148	1	1	1	1	1				


  select * from etsy-bigquery-adhoc-prod._scriptd26af5f9b3fcea653e8d093c64bd2866eef58a64.first_visits_users where (mapped_user_id = 678931407 or mapped_user_id = 954602148 or mapped_user_id =48422026)
-- browser_platform	region	first_app_visit	start_datetime	mapped_user_id	next_visit_date
-- iOS	GB	2024-07-23	2024-07-23 12:41:08.000000 UTC	954602148	2024-07-23
-- iOS	US	2024-07-06	2024-07-06 16:32:55.000000 UTC	678931407	2024-07-18



-----BROWSER TESTING
with yoy_union as (
select 
  'ty' as era,
  first_app_visit,
  browser_platform,
  region,
  browser_id,
  is_signed_in,
  count(distinct browser_id) as browsers_with_first_visit,
  count(distinct case when a.first_app_visit = next_visit_date then browser_id end) as browsers_visit_in_same_day,
  count(distinct case when next_visit_date <= a.first_app_visit + 6 then browser_id end) as browsers_visit_in_first_7_days,
  count(distinct case when next_visit_date <= a.first_app_visit + 13 then browser_id end) as browsers_visit_in_first_14_days,
  count(distinct case when next_visit_date <= a.first_app_visit + 29 then browser_id end) as browsers_visit_in_first_30_days,
from 
  etsy-bigquery-adhoc-prod._scriptd26af5f9b3fcea653e8d093c64bd2866eef58a64.first_visits_browsers a
group by all
union all
select
  'ly' as era,
  CAST(date_add(a.first_app_visit, interval 52 WEEK) as DATETIME) AS first_app_visit,
  browser_platform,
  region,
    browser_id,
  is_signed_in,
  count(distinct browser_id) as browsers_with_first_visit,
  count(distinct case when a.first_app_visit = next_visit_date then browser_id end) as browsers_visit_in_same_day,
  count(distinct case when next_visit_date <= a.first_app_visit + 6 then browser_id end) as browsers_visit_in_first_7_days,
  count(distinct case when next_visit_date <= a.first_app_visit + 13 then browser_id end) as browsers_visit_in_first_14_days,
  count(distinct case when next_visit_date <= a.first_app_visit + 29 then browser_id end) as browsers_visit_in_first_30_days,
from 
  etsy-bigquery-adhoc-prod._scriptd26af5f9b3fcea653e8d093c64bd2866eef58a64.first_visits_browsers a 
group by all 
)
SELECT
  cast(first_app_visit as date) as first_app_visit,
  browser_platform,
  region,
    browser_id,
  is_signed_in,
  --ty metrics
  sum(CASE WHEN era = 'ty' THEN browsers_with_first_visit END) AS ty_browsers_with_first_visit,
  sum(CASE WHEN era = 'ty' THEN browsers_visit_in_same_day END) AS ty_browsers_visit_in_same_day,
  sum(CASE WHEN era = 'ty' THEN browsers_visit_in_first_7_days END) AS ty_browsers_visit_in_first_7_days,
  sum(CASE WHEN era = 'ty' THEN browsers_visit_in_first_14_days END) AS ty_browsers_visit_in_first_14_days,
  sum(CASE WHEN era = 'ty' THEN browsers_visit_in_first_30_days END) AS ty_browsers_visit_in_first_30_days,
  --ly metrics
  sum(CASE WHEN era = 'ly' THEN browsers_with_first_visit END) AS ly_browsers_with_first_visit,
  sum(CASE WHEN era = 'ly' THEN browsers_visit_in_first_7_days END) AS ly_browsers_visit_in_first_7_days,
  sum(CASE WHEN era = 'ly' THEN browsers_visit_in_first_14_days END) AS ly_browsers_visit_in_first_14_days,
  sum(CASE WHEN era = 'ly' THEN browsers_visit_in_first_30_days END) AS ly_browsers_visit_in_first_30_days,
  FROM
    yoy_union
  WHERE first_app_visit < CAST(current_date() as DATETIME)
  GROUP BY all

---4Y7ztkp31tinfHGn-1sBNxPaChTj, not signed in , not visit within 30 days
---3EA5D46E1F3B40C9BF72DF4C51E3, not signed in ,  visit within 30 days  
---JSjrE-6-TaWW6hTVmnpFog, signed in , not visit within 30 days

select * from etsy-bigquery-adhoc-prod._scriptd26af5f9b3fcea653e8d093c64bd2866eef58a64.first_visits_browsers where browser_id in ('4Y7ztkp31tinfHGn-1sBNxPaChTj','3EA5D46E1F3B40C9BF72DF4C51E3','JSjrE-6-TaWW6hTVmnpFog')
-- browser_platform	region	first_app_visit	start_datetime	browser_id	is_signed_in	next_visit_date
-- iOS	TR	2024-07-31	2024-07-31 09:29:16.000000 UTC	3EA5D46E1F3B40C9BF72DF4C51E3	0	2024-08-18
-- Android	TT	2024-07-19	2024-07-19 13:10:55.000000 UTC	4Y7ztkp31tinfHGn-1sBNxPaChTj	0	
-- Android	US	2024-07-05	2024-07-05 19:49:49.000000 UTC	JSjrE-6-TaWW6hTVmnpFog	1	2024-08-14


  select
  browser_id,
  start_datetime,
  user_id,
  v._date,
  lead(v._date) over (partition by v.browser_id order by v.start_datetime asc) as next_visit_date
from 
  `etsy-data-warehouse-prod.weblog.visits` v  
where 
  v.platform = "boe"
  and v._date is not null 
  and v.event_source in ("ios", "android")
  and v.platform in ('boe')
  and v._date >= current_date-60
  and browser_id in ('4Y7ztkp31tinfHGn-1sBNxPaChTj','3EA5D46E1F3B40C9BF72DF4C51E3','JSjrE-6-TaWW6hTVmnpFog')
group by all
qualify row_number() over(partition by v.browser_id order by start_datetime) = 1
-- browser_id	start_datetime	user_id	_date	next_visit_date
-- 4Y7ztkp31tinfHGn-1sBNxPaChTj	2024-07-19 13:10:55.000000 UTC		2024-07-19	
-- JSjrE-6-TaWW6hTVmnpFog	2024-07-05 19:49:49.000000 UTC	820933413	2024-07-05	2024-08-14
-- 3EA5D46E1F3B40C9BF72DF4C51E3	2024-07-31 09:29:16.000000 UTC		2024-07-31	2024-08-18

-----------------CHECKING TO MAKE SURE YOY CALCS WORK RIGHT
select sum(ty_users_with_first_visit)/ nullif(sum(ly_users_with_first_visit),0)-1 
from etsy-data-warehouse-dev.rollups.boe_user_retention_yoy 
----DOES NOT = 1


select * from etsy-data-warehouse-dev.rollups.boe_user_retention_yoy 
where ty_users_with_first_visit > 5 and ly_users_with_first_visit > 5
limit 20
-- first_app_visit	browser_platform	region	buyer_segment	ty_users_with_first_visit	ty_users_visit_in_same_day	ty_users_visit_in_first_7_days	ty_users_visit_in_first_14_days	ty_users_visit_in_first_30_days	ly_users_with_first_visit	ly_users_visit_in_first_7_days	ly_users_visit_in_first_14_days	ly_users_visit_in_first_30_days
-- 2021-10-06	      Android	            NO	    Lapsed	        13	                          1	                            1	                          1	                                        2	                      10	                        0	                            0	                                0
-- 2021-10-05	Android	MX	Lapsed	9	2	2	2	2	7	0	0	0
-- 2023-05-19	Android	DO	Lapsed	43	1	2	2	3	9	0	0	0
-- 2021-10-06	Android	DK	Lapsed	10	1	2	2	2	9	0	0	0
-- 2022-08-29	unknown	CA	Zero Time	6	1	2	2	2	9	0	0	2
-- 2022-10-22	Android	SG	Lapsed	8	2	3	3	3	7	0	0	1
-- 2022-10-19	Android	BR	OTB	8	0	4	4	4	21	0	0	1
-- 2019-12-11	iOS	FI	Zero Time	7	3	4	4	5	7	0	0	0
-- 2020-07-31	iOS	EG	Zero Time	7	3	4	4	5	7	0	0	0
-- 2023-02-20	Android	PK	Zero Time	10	1	4	4	4	7	0	0	1
-- 2022-10-19	Android	BD	Lapsed	12	1	5	5	5	6	0	0	0
-- 2021-02-16	Android	AR	Zero Time	7	4	5	5	5	6	0	0	0
-- 2024-01-19	Android	BE	Lapsed	7	5	6	6	6	6	0	0	1
  
\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
  
-- select CAST(date_sub(date('2024-08-01'), interval 52 WEEK) as DATETIME)

------------with where filter on date
select sum(ty_users_with_first_visit)/ nullif(sum(ly_users_with_first_visit),0)-1 
from etsy-data-warehouse-dev.rollups.boe_user_retention_yoy 
--0.17968680759722067

select * from etsy-data-warehouse-dev.rollups.boe_user_retention_yoy
where (first_app_visit = '2024-08-01' or first_app_visit = '2023-08-03') -- 8/3/23 is 52 weeks after 8/1/2024
and browser_platform in ('iOS')
and region in ('US')
and buyer_segment in ('Zero Time')
-- first_app_visit	browser_platform	region	buyer_segment	ty_users_with_first_visit	ty_users_visit_in_same_day	ty_users_visit_in_first_7_days	ty_users_visit_in_first_14_days	ty_users_visit_in_first_30_days	ly_users_with_first_visit	ly_users_visit_in_same_day	ly_users_visit_in_first_7_days	ly_users_visit_in_first_14_days	ly_users_visit_in_first_30_days
-- 2024-08-01	iOS	US	Zero Time	9674	3523	6227	6821	7322	15005	5011	8896	9729	10601
-- 2023-08-03	iOS	US	Zero Time	15005	5011	8896	9729	10601	17035	11339	13832	14432	15012


------------without where filter on date
------------without where filter on date
select sum(ty_users_with_first_visit)/ nullif(sum(ly_users_with_first_visit),0)-1 
from etsy-data-warehouse-dev.rollups.boe_user_retention_yoy 
--0.0

select * from etsy-data-warehouse-dev.rollups.boe_user_retention_yoy
where (first_app_visit = '2024-08-01' or first_app_visit = '2023-08-03') -- 8/3/23 is 52 weeks after 8/1/2024
and browser_platform in ('iOS')
and region in ('US')
and buyer_segment in ('Zero Time')
-- first_app_visit	browser_platform	region	buyer_segment	ty_users_with_first_visit	ty_users_visit_in_same_day	ty_users_visit_in_first_7_days	ty_users_visit_in_first_14_days	ty_users_visit_in_first_30_days	ly_users_with_first_visit	ly_users_visit_in_same_day	ly_users_visit_in_first_7_days	ly_users_visit_in_first_14_days	ly_users_visit_in_first_30_days
-- 2024-08-01	iOS	US	Zero Time	9674	3523	6227	6821	7322	15005	5011	8896	9729	10601
-- 2023-08-03	iOS	US	Zero Time	15005	5011	8896	9729	10601	17035	11339	13832	14432	15012


-----testing user counts across fields 
select first_app_visit,
sum(ty_users_with_first_visit) as ty_users_with_first_visit,
sum(ty_users_visit_in_same_day) as ty_users_visit_in_same_day,
sum(ty_users_visit_in_first_7_days) as ty_users_visit_in_first_7_days,
sum(ty_users_visit_in_first_14_days) as ty_users_visit_in_first_14_days,
sum(ty_users_visit_in_first_30_days) as ty_users_visit_in_first_30_days,
sum(ly_users_with_first_visit) as ly_users_with_first_visit,
sum(ly_users_visit_in_same_day) as ly_users_visit_in_same_day,
sum(ly_users_visit_in_first_7_days) as ly_users_visit_in_first_7_days,
sum(ly_users_visit_in_first_14_days) as ly_users_visit_in_first_14_days,
sum(ly_users_visit_in_first_30_days) as ly_users_visit_in_first_30_days,
from etsy-data-warehouse-prod.rollups.boe_user_retention_yoy
where (first_app_visit = '2024-08-01' or first_app_visit = '2023-08-03') -- 8/3/23 is 52 weeks after 8/1/2024
group by all 
-- first_app_visit	ty_users_with_first_visit	ty_users_visit_in_same_day	ty_users_visit_in_first_7_days	ty_users_visit_in_first_14_days	ty_users_visit_in_first_30_days	ly_users_with_first_visit	ly_users_visit_in_same_day	ly_users_visit_in_first_7_days	ly_users_visit_in_first_14_days	ly_users_visit_in_first_30_days
-- 2023-08-03	60834	22438	40486	43767	46780	70551	37991	54161	57367	60283
-- 2024-08-01	46099	16921	30367	32912	34975	60834	22438	40486	43767	46780
