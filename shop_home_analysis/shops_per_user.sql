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
