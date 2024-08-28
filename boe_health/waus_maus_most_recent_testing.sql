-- select month, mapped_user_id, count(*) from etsy-data-warehouse-dev.rollups.boe_maus_retention group by all order by 3 desc

select * from  etsy-data-warehouse-dev.rollups.boe_maus_retention where mapped_user_id = 929317673
-- month	buyer_segment	top_channel	browser_platform	region	mapped_user_id	ty_maus	ty_retained	ly_maus	ly_retained
-- 2024-07-01	Active	direct	iOS	FR	929317673	1	0		
-- 2024-06-01	Active	direct	iOS	FR	929317673	1	1		
-- 2024-05-01	Active	direct	iOS	FR	929317673	1	1		
