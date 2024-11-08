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
