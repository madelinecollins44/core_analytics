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
