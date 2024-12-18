--Where do they go after shop home?  
----Prior screen, segment by visitors that purchase in-session vs. not, purchase something from the shop vs. not

-----------------------------------------------------------------------------------------------------------------------------------------------
--overall traffic from shop home
---------------------------------------------------------------------------------------------------------------------------------------------
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

-----------------------------------------------------------------------------------------------------------------------------------------------
--visits that convert
---------------------------------------------------------------------------------------------------------------------------------------------
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

-----------------------------------------------------------------------------------------------------------------------------------------------
--where do landings come from 
---------------------------------------------------------------------------------------------------------------------------------------------
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
	
-----------------------------------------------------------------------------------------------------------------------------------------------
--looking at visits that have purchased from that shop
---------------------------------------------------------------------------------------------------------------------------------------------
--get visit info of when a visit see a shop_home page
with purchased_from_shops as (
select
  tv.visit_id, 
	t.seller_user_id,
	cast(sb.shop_id as string) as shop_id,
  count(transaction_id) as transactions
from 
  etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
inner join
	etsy-data-warehouse-prod.transaction_mart.all_transactions t 
		using (transaction_id)
left join etsy-data-warehouse-prod.rollups.seller_basics sb
	on t.seller_user_id=sb.user_id
where tv.date >= current_date-30
group by all 
)
, visits_to_home_and_purchase as (
select
 b.visit_id,
 b.sequence_number, -- need this so can join to next page
 b.shop_id,
 a.transactions
from 
  etsy-data-warehouse-dev.madelinecollins.visited_shop_ids b 
inner join 
  purchased_from_shops a using (visit_id, shop_id)
group by all
)
, previous_page as (
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
--look at the next_page for anyone that views the shop_home page + has purchased from that shop in visit
select 
	np.previous_page,
	-- np.event_type,
	count(vh.visit_id) as pageviews,
	count(distinct vh.visit_id) as unique_visits
from visits_to_home_and_purchase vh
inner join previous_page np using (visit_id, sequence_number)
group by all

-----------------------------------------------------------------------------------------------------------------------------------------------
---share of total listing views that are from shop home 
---------------------------------------------------------------------------------------------------------------------------------------------
with shop_home_visits as (
select distinct
  visit_id
from etsy-data-warehouse-prod.weblog.events
where event_type in ('shop_home')
and _date>= current_date-30
)
select
  count(distinct case when landing_event in ('shop_home') then visit_id end) as shop_home_landings,
  count(distinct all_visits.visit_id) as all_visits,
  count(distinct case when landing_event in ('shop_home') then visit_id end) /  count(distinct all_visits.visit_id) as share
from shop_home_visits  all_visits 
inner join etsy-data-warehouse-prod.weblog.visits using (visit_id)
where _date >= current_date-30

-----------------------------------------------------------------------------------------------------------------------------------------------
--share of pageviews that are from visits that land on shop_home
---------------------------------------------------------------------------------------------------------------------------------------------
with shop_home_landings as (
select 
  visit_id
from etsy-data-warehouse-prod.weblog.visits
where landing_event in ('shop_home')
and _date>= current_date-30
)
select
  count(e.visit_id) as shop_home_pageviews,
  count(case when l.visit_id is not null then e.visit_id end) as pageviews_from_landing_visits,
  coalesce(count(case when l.visit_id is not null then e.visit_id end)/ count(e.visit_id) ,0) as share_
from etsy-data-warehouse-prod.weblog.events e 
left join shop_home_landings l using (visit_id)
where 
  e._date >= current_date-30
  and event_type in ('shop_home')
-- shop_home_pageviews	pageviews_from_landing_visits	share_
-- 405956651	141133426	0.347656395460805

-----------------------------------------------------------------------------------------------------------------------------------------------
---what % of total pageviews, shop home pageviews come from each landing page
---------------------------------------------------------------------------------------------------------------------------------------------
with shop_home_landings as (
select 
  landing_event, 
  visit_id
from etsy-data-warehouse-prod.weblog.visits
where landing_event in ('shop_home')
and _date>= current_date-30
)
select
  landing_event,
  count(e.visit_id) as all_pageviews,
  count(case when event_type in ('shop_home') then e.visit_id end) as shop_home_pageviews,
from etsy-data-warehouse-prod.weblog.events e 
left join shop_home_landings l using (visit_id)
where 
  e._date >= current_date-30
group by all

-----------------------------------------------------------------------------------------------------------------------------------------------
--what % of landings are shop home
---------------------------------------------------------------------------------------------------------------------------------------------
select 
  landing_event, 
  count(distinct visit_id) as visits
from etsy-data-warehouse-prod.weblog.visits
where _date>= current_date-30
group by all


