------------------------------------------------------
--how many different shops do users purchase from?
------------------------------------------------------
with purchased_from_shops as (
select
  tv.user_id, 
	count(distinct t.seller_user_id) as unique_sellers,
  count(distinct transaction_id) as unique_transactions
  -- count(distinct t.transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
inner join
	etsy-data-warehouse-prod.transaction_mart.all_transactions t 
		using (transaction_id)
where 
  tv.date >= current_date-30
group by all 
)
select
  count(distinct user_id) as unique_users,
  avg(unique_sellers) as avg_unique_sellers,
  avg(unique_transactions) as avg_unique_transactions
from 
  purchased_from_shops

------------------------------------------------------------------------------------------------------------
--repeat purchases from the same shop - are there lots of shop ‘loyalists’?
------------------------------------------------------------------------------------------------------------
with purchased_from_shops as (
select
  tv.user_id, 
	t.seller_user_id,
  count(distinct transaction_id) as unique_transactions
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
inner join
	etsy-data-warehouse-prod.transaction_mart.all_transactions t 
		using (transaction_id)
where 
  tv.date >= current_date-365
group by all 
)
select
  count(distinct case when unique_transactions = 1 then user_id end) as users_purchase_from_shop_1_time,
  count(distinct case when unique_transactions = 2 then user_id end) as users_purchase_from_shop_2_times,
  count(distinct case when unique_transactions = 3 then user_id end) as users_purchase_from_shop_3_times,
  count(distinct case when unique_transactions = 4 then user_id end) as users_purchase_from_shop_4_times,
  count(distinct case when unique_transactions = 5 then user_id end) as users_purchase_from_shop_5_times,  
  count(distinct case when unique_transactions = 10 then user_id end) as users_purchase_from_shop_10_times,
  count(distinct case when unique_transactions = 15 then user_id end) as users_purchase_from_shop_15_times,
  count(distinct case when unique_transactions = 20 then user_id end) as users_purchase_from_shop_20_times,  
  count(distinct case when unique_transactions = 30 then user_id end) as users_purchase_from_shop_30_times
from purchased_from_shops

------------------------------------------------------------------------------------------------------------
--testing
------------------------------------------------------------------------------------------------------------
select * 
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
inner join
	etsy-data-warehouse-prod.transaction_mart.all_transactions t 
		using (transaction_id)
where 
  tv.date >= current_date-365
  and user_id = 110492431
  and seller_user_id = 736070039
  -- user_id	seller_user_id	unique_transactions
-- 110492431	736070039	1
-- 237489707	21966421	1
-- 697420773	345387332	1
-- 41142537	342735968	1
-- 994626323	499669497	1
-- 29923559	365353078	20
-- 107218282	936909057	20
-- 901681355	880990572	20
-- 23140470	46773266	20
-- 644000470	74161633	20
-- 7175522	262736160	7
-- 22322565	767097900	7
-- 20203061	580326942	7
-- 371554703	44152313	7
-- 80859636	17613142	7
