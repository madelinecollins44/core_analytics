with shop_tiers as (
select
  vs.raw_shop_shop_id,
  sb.seller_tier_new,
  sb.power_shop_status,
  sb.top_shop_status,
  sb.medium_shop_status,
  sb.small_shop_status
from 
  (select distinct raw_shop_shop_id from etsy-data-warehouse-dev.madelinecollins.visited_shop_ids) vs
left join 
  etsy-data-warehouse-prod.rollups.seller_basics sb 
    on vs.raw_shop_shop_id= cast(sb.shop_id as string)
group by all
)
--need to get shop_ids to visit level
, pageviews_per_shop as (
select
  raw_shop_shop_id,
  visit_id,
  count(sequence_number) as pageviews
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids
group by all
)
, add_in_gms as (
select
  a.raw_shop_shop_id,
  b.browser_platform,
  platform,
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
, visit_level_metrics as (
select
  raw_shop_shop_id,
  browser_platform,
  platform,
  count(distinct visit_id) as unique_visits,
  sum(pageviews) as pageviews,
  sum(total_gms) as total_gms,
from add_in_gms
group by all 
)
select
  seller_tier_new,
  browser_platform,
  platform,
  count(distinct a.raw_shop_shop_id) as visited_shops,
  sum(unique_visits) as total_visits,
  sum(pageviews) as pageviews,
  sum(a.total_gms) as total_gms
from 
  visit_level_metrics a
left join 
  shop_tiers b using (raw_shop_shop_id)
group by all 
