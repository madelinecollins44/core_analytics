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
  count(distinct visit_id) as visits
from 
  shop_home_visits
where 
  event_type in ('shop_home')
group by all 
order by 2 desc 

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

