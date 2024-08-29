-- select month, mapped_user_id, count(*) from etsy-data-warehouse-dev.rollups.boe_maus_retention group by all order by 3 desc

select * from  etsy-data-warehouse-dev.rollups.boe_maus_retention where mapped_user_id = 929317673
-- month	buyer_segment	top_channel	browser_platform	region	mapped_user_id	ty_maus	ty_retained	ly_maus	ly_retained
-- 2024-07-01	Active	direct	iOS	FR	929317673	1	0		
-- 2024-06-01	Active	direct	iOS	FR	929317673	1	1		
-- 2024-05-01	Active	direct	iOS	FR	929317673	1	1		

--------
select week, sum(ty_waus) as ty_waus, sum(ty_retained) as ty_retained, sum(ly_waus) as ly_waus, sum(ly_retained) as ly_retained 
  from etsy-data-warehouse-dev.rollups.boe_waus_retention 
  where week = '2024-08-25' or week= '2023-08-27' 
  group by all 
-- week	ty_waus	ty_retained	ly_waus	ly_retained
-- 2023-08-27	16874082	9966875	14957418	8980775
-- 2024-08-25	10599418	0	16874082	9966875


-----test specific user id
select * from etsy-data-warehouse-dev.rollups.boe_waus_retention_most_recent where mapped_user_id = 532632418 order by week asc
-- week	buyer_segment	top_channel	browser_platform	region	mapped_user_id	ty_waus	ty_retained	ly_waus	ly_retained
-- 2022-03-27	Repeat	direct	iOS	US	532632418	1	0		
-- 2022-04-17	Repeat	direct	iOS	US	532632418	1	0		
-- 2022-05-08	Repeat	direct	iOS	US	532632418	1	0		
-- 2022-05-29	Repeat	direct	iOS	US	532632418	1	0		
-- 2022-06-19	Repeat	direct	iOS	US	532632418	1	1		
-- 2022-06-26	Repeat	direct	iOS	US	532632418	1	0		
-- 2022-07-17	Repeat	direct	iOS	US	532632418	1	0		
-- 2022-08-07	Repeat	direct	iOS	US	532632418	1	1		
-- 2022-08-14	Repeat	direct	iOS	US	532632418	1	0		
-- 2022-08-28	Repeat	direct	iOS	US	532632418	1	1		
-- 2022-09-04	Repeat	direct	iOS	US	532632418	1	0		
-- 2022-10-02	Repeat	direct	iOS	US	532632418	1	0		
-- 2022-10-30	Repeat	direct	iOS	US	532632418	1	1		
-- 2022-11-06	Repeat	direct	iOS	US	532632418	1	0		
-- 2022-11-20	Repeat	direct	iOS	US	532632418	1	0		
-- 2022-12-11	Repeat	direct	iOS	US	532632418	1	1		
-- 2022-12-18	Repeat	direct	iOS	US	532632418	1	1		
-- 2022-12-25	Repeat	direct	iOS	US	532632418	1	0		
-- 2023-01-22	Repeat	direct	iOS	US	532632418	1	1		
-- 2023-01-29	Repeat	direct	iOS	US	532632418	1	0		
-- 2023-02-19	Repeat	direct	iOS	US	532632418	1	1		
-- 2023-02-26	Repeat	direct	iOS	US	532632418	1	0		
-- 2023-03-26	Repeat	direct	iOS	US	532632418	1	1	1	0
-- 2023-04-02	Repeat	direct	iOS	US	532632418	1	1		
-- 2023-04-09	Repeat	direct	iOS	US	532632418	1	1		
-- 2023-04-16	Repeat	direct	iOS	US	532632418	1	0	1	0
-- 2023-05-07	Repeat	direct	iOS	US	532632418	1	1	1	0
-- 2023-05-14	Repeat	direct	iOS	US	532632418	1	1		
-- 2023-05-21	Repeat	direct	iOS	US	532632418	1	0		
-- 2023-05-28	Repeat	direct	iOS	US	532632418			1	0
-- 2023-06-18	Repeat	direct	iOS	US	532632418	1	1	1	1
-- 2023-06-25	Repeat	direct	iOS	US	532632418	1	0	1	0
-- 2023-07-16	Repeat	direct	iOS	US	532632418	1	0	1	0
-- 2023-07-30	Repeat	direct	iOS	US	532632418	1	1		
-- 2023-08-06	Repeat	direct	iOS	US	532632418	1	0	1	1
-- 2023-08-13	Repeat	direct	iOS	US	532632418			1	0
-- 2023-08-27	Repeat	direct	iOS	US	532632418			1	1
-- 2023-09-03	Repeat	direct	iOS	US	532632418			1	0
-- 2023-09-10	Repeat	direct	iOS	US	532632418	1	1		
-- 2023-09-17	Repeat	direct	iOS	US	532632418	1	0		
-- 2023-10-01	Repeat	direct	iOS	US	532632418			1	0
-- 2023-10-22	Repeat	direct	iOS	US	532632418	1	1		
-- 2023-10-29	Repeat	direct	iOS	US	532632418	1	1	1	1
-- 2023-11-05	Repeat	direct	iOS	US	532632418	1	0	1	0
