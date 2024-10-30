-----------------------------------------------------------------------------------------------------------------------------------------------
--Where do they go after shop home?  
----Prior screen, segment by visitors that purchase in-session vs. not, purchase something from the shop vs. not
---------------------------------------------------------------------------------------------------------------------------------------------
--overall traffic from shop home
with shop_home_visits as (
select
  visit_id,
  sequence_number,
  event_type,
  lag(event_type) over (partition by visit_id order by sequence_number) as previous_page
from 
  etsy-data-warehouse-prod.weblog.events
where
  _date >= current_date-30
  and page_view=1 
)
select
  previous_page,
  count(visit_id) as pageviews,
  count(distinct visit_id) as unique_visits
from 
  shop_home_visits
where 
  event_type in ('shop_home')
group by all 
order by 2 desc 

--visits that convert 
--visits that convert 
with shop_home_visits as (
select
  visit_id,
  sequence_number,
  event_type,
  lag(event_type) over (partition by visit_id order by sequence_number) as previous_page
from 
  etsy-data-warehouse-prod.weblog.events
where
  _date >= current_date-30
  and page_view=1 
)
select
  previous_page,
  count(visit_id) as pageviews,
  count(distinct visit_id) as visits
from 
  shop_home_visits shv
inner join 
  etsy-data-warehouse-prod.weblog.visits v using (visit_id)
where 
  event_type in ('shop_home')
  and v.converted > 0 
  and v._date >= current_date-30
group by all 
order by 2 desc 

--where do landings come from?
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
  count(distinct visit_id) as visits
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30
and landing_event in ('shop_home')
group by all 

---looking at visits that have purchased from that shop
--find visit_ids, shop_ids, sequence number
with visited_shop_ids as (
select
  visit_id
	, sequence_number
	, (select value from unnest(beacon.properties.key_value) where key = "shop_id") as shop_id
	, (select value from unnest(beacon.properties.key_value) where key = "shop_shop_id") as shop_shop_id
from 
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` 
where 
  beacon.event_name in ('shop_home')
  and date(_partitiontime) >= current_date-30
)
, purchased_from_shops as (
select
  tv.visit_id, 
	t.seller_user_id,
	cast(sb.shop_id as string) as shop_id
	--shop_id here 
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
inner join
	etsy-data-warehouse-prod.transaction_mart.all_transactions t 
		using (transaction_id)
left join etsy-data-warehouse-prod.rollups.seller_basics sb
	on t.seller_user_id=sb.user_id
)
-- how many visitors have actually purchased on the shop 
select
  count(distinct a.visit_id) as shop_purchasers,
  count(distinct b.visit_id) as purchasers_that_have_visited
from purchased_from_shops a
left join visited_shop_ids b using (visit_id, shop_id)
