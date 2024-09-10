------------------------------------------------
--see if new rollup matches old one 
------------------------------------------------
select sum(ty_waus) as waus, sum(ty_retained) as retained, sum(ly_waus) as ly_waus, sum(ly_retained) as ly_retained from etsy-data-warehouse-prod.rollups.boe_waus_retention
where week in ('2024-06-10')
-- waus	      retained    	ly_waus	      ly_retained
-- 16212949	  8844638	      16061731	      9177131

select sum(ty_waus) as waus, sum(ty_retained) as retained, sum(ly_waus) as ly_waus, sum(ly_retained) as ly_retained from `etsy-data-warehouse-dev.madelinecollins.waus_retention_recent` 
where week in ('2024-06-10')
-- waus	      retained	    ly_waus     	ly_retained
-- 16212949	    8844638	    16061731	      9177131

--testing waus 
select 
  week
  , sum(ty_waus) as ty_waus
  , sum(ty_retained) as ty_retained
  , sum(ly_waus) as ly_waus
  , sum(ly_retained) as ly_retained
from etsy-data-warehouse-prod.rollups.boe_waus_retention
where week in ('2024-01-01') or week in ('2024-04-15')
group by all
-------prod-------
-- week	        ty_waus	        ty_retained	    ly_waus	    ly_retained
-- 2024-01-01	18681942	    10882227	    17700891	10740126
-- 2024-04-15	16398089	    9145228	        16626958	9673011
-------dev-------
-- week	        ty_waus	        ty_retained	      ly_waus	    ly_retained
-- 2024-01-01	18681942	    10882227	      17700891	    10740126
-- 2024-04-15	16398089	    9145228	          16626958	    9673011


--testing maus 
select 
  month
  , sum(ty_maus) as ty_maus
  , sum(ty_retained) as ty_retained
  , sum(ly_maus) as ly_maus
  , sum(ly_retained) as ly_retained
from etsy-data-warehouse-dev.rollups.boe_maus_retention
where month in ('2024-01-01') or month in ('2024-04-01')
group by all
    ----dev
-- month	      ty_maus      	ty_retained	      ly_maus	      ly_retained
-- 2024-04-01	    33257728	    23709553	      32761129	      23220416
-- 2024-01-01	    35575628	    25314507	      32920647	      23947126
 ----prod
-- month	          ty_maus	      ty_retained	      ly_maus	      ly_retained
-- 2024-01-01	      35575628	      25314507	      32920647	      23947126
-- 2024-04-01	      33257728	      23709553	      32761129	      23220416
    
----testing to see if row counting is correct
with agg as (select 
    v._date
    , date_trunc(v._date, week(MONDAY)) as week
    , date_trunc(v._date, month) as month
    , m.mapped_user_id 
    , v.top_channel
    , v.browser_platform
    , v.region
    , v.visit_id
    , v.total_gms
    , row_number() over (partition by m.mapped_user_id, date_trunc(v._date, week(MONDAY)) order by _date) as visit_number_week
    , row_number() over (partition by m.mapped_user_id, date_trunc(v._date, month) order by _date) as visit_number_month
  from etsy-data-warehouse-prod.weblog.visits v
  join etsy-data-warehouse-prod.user_mart.mapped_user_profile m using (user_id)
  where 
    platform in ('boe') 
    and _date >= current_date-880 
    -- and v.user_id is not null
    and mapped_user_id= 532632418)
    -- select month, count(*) from agg group by all order by 2 desc
    select * from agg where month = 	'2023-04-01'
    -- visit_number_week =1 
-- _date	week	month	mapped_user_id	top_channel	browser_platform	region	visit_id	total_gms	visit_number_week	visit_number_month
-- 2023-04-01	2023-03-27	2023-04-01	532632418	direct	iOS	US	5F28F26218894C8381351D0546D1.1680369743123.1	0	3	1
-- 2023-04-03	2023-04-03	2023-04-01	532632418	direct	unknown	US	5F28F26218894C8381351D0546D1.1680519323215.1	0	1	2
-- 2023-04-04	2023-04-03	2023-04-01	532632418	direct	iOS	US	5F28F26218894C8381351D0546D1.1680624461219.1	0	2	3
-- 2023-04-04	2023-04-03	2023-04-01	532632418	direct	iOS	US	5F28F26218894C8381351D0546D1.1680641267667.1	0	3	4
-- 2023-04-04	2023-04-03	2023-04-01	532632418	us_paid	iOS	US	5F28F26218894C8381351D0546D1.1680626547488.2	0	4	5
-- 2023-04-05	2023-04-03	2023-04-01	532632418	direct	iOS	US	5F28F26218894C8381351D0546D1.1680727674838.1	11.99	5	6
-- 2023-04-05	2023-04-03	2023-04-01	532632418	direct	iOS	US	5F28F26218894C8381351D0546D1.1680733952789.1	0	6	7
-- 2023-04-07	2023-04-03	2023-04-01	532632418	direct	iOS	US	5F28F26218894C8381351D0546D1.1680901487271.1	0	7	8
-- 2023-04-08	2023-04-03	2023-04-01	532632418	direct	iOS	US	5F28F26218894C8381351D0546D1.1680959917164.1	0	8	9
-- 2023-04-08	2023-04-03	2023-04-01	532632418	direct	iOS	US	5F28F26218894C8381351D0546D1.1680977378304.1	0	9	10
-- 2023-04-08	2023-04-03	2023-04-01	532632418	direct	iOS	US	5F28F26218894C8381351D0546D1.1680986370211.1	0	10	11
-- 2023-04-08	2023-04-03	2023-04-01	532632418	us_paid	iOS	US	5F28F26218894C8381351D0546D1.1680977426349.2	0	11	12
