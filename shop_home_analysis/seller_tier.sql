-------GIVEN THAT SHOP_IDS ARE MORE RELIABLE THAT SELLER_USER_IDS, USING SHOP_IDS INSTEAD OF USER_IDS
---------------------------------------------------------------------------
--overall traffic by shop type, landing traffic by shop type
---------------------------------------------------------------------------
---------------------------------------------------------------------------
--overall traffic by shop type, landing traffic by shop type
---------------------------------------------------------------------------
with shop_tiers as (
select
  vs.shop_id,
  sb.seller_tier_new,
  sb.power_shop_status,
  sb.top_shop_status,
  sb.medium_shop_status,
  sb.small_shop_status
from 
  (select distinct shop_id from etsy-data-warehouse-dev.madelinecollins.visited_shop_ids) vs
left join 
  etsy-data-warehouse-prod.rollups.seller_basics sb 
    on vs.shop_id= cast(sb.shop_id as string)
group by all
)
-- 7069431 shop_ids, 710219 shop_ids without tiers, 10.04% without a match  


--need to get shop_ids to visit level
, 
with pageviews_per_shop as (
select
  shop_id,
  visit_id,
  count(sequence_number) as pageviews
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids
group by all
)
-- 7069431 shop_ids, 155315719 visits 
, add_in_gms as (
select
  a.shop_id,
  a.visit_id,
  a.pageviews,
  sum(b.total_gms) as total_gms
from 
  pageviews_per_shop a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  _date >= current_date-30
group by all 
)
-- 7050278 shop_ids, 152877206 visits 
, visit_level_metrics as (
select
  shop_id,
  count(distinct visit_id) as unique_visits,
  sum(pageviews) as pageviews,
  sum(total_gms) as total_gms,
from add_in_gms
group by all 
)
select count(distinct shop_id), count(distinct visit_id) from add_in_gms group by all 

, agg as (select
  seller_tier_new,
  count(distinct a.shop_id) as visited_shops,
  sum(unique_visits) as total_visits,
  sum(pageviews) as pageviews,
  sum(a.total_gms) as total_gms
from 
  visit_level_metrics a
left join 
  shop_tiers b using (shop_id)
group by all 
)
select sum(visited_shops) from agg
--7050278
--
----------------------------------------------------------------
--by reporting channel
----------------------------------------------------------------
with shop_tiers as (
select
  vs.shop_id,
  sb.seller_tier_new,
  sb.power_shop_status,
  sb.top_shop_status,
  sb.medium_shop_status,
  sb.small_shop_status
from 
  (select distinct shop_id from etsy-data-warehouse-dev.madelinecollins.visited_shop_ids) vs
left join 
  etsy-data-warehouse-prod.rollups.seller_basics sb 
    on vs.shop_id= cast(sb.shop_id as string)
group by all
)
-- 7069431 shop_ids, 710219 shop_ids without tiers, 10.04% without a match  
, pageviews_per_shop as (
select
  shop_id,
  visit_id,
  count(sequence_number) as pageviews
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids
group by all
)
-- 7069431 shop_ids, 155315719 visits 
, add_in_gms as (
select
 case 
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
      end as reporting_channel, 
  a.shop_id,
  a.visit_id,
  a.pageviews,
  -- case when b.visit_id is null then 1 else 0 end as ordered,
  sum(b.total_gms) as total_gms
from 
  pageviews_per_shop a
inner join 
  etsy-data-warehouse-prod.weblog.visits b using (visit_id)
where 
  _date >= current_date-30
group by all 
)
-- 7050278 shop_ids, 152877206 visits 
, visit_level_metrics as (
select
  reporting_channel,
  shop_id,
  count(distinct visit_id) as unique_visits,
  sum(pageviews) as pageviews,
  sum(total_gms) as total_gms,
from add_in_gms
group by all 
)
select
  reporting_channel,
  case when seller_tier_new is null then "null" else seller_tier_new end as seller_tier_new,
  count(distinct a.shop_id) as visited_shops,
  sum(unique_visits) as total_visits,
  sum(pageviews) as pageviews,
  sum(a.total_gms) as total_gms
from 
  visit_level_metrics a
left join 
  shop_tiers b using (shop_id)
group by all 
