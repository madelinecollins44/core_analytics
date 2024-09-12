  --  QUESTION TO ANSWER: Look into what types of users we’re driving to the app via push, and how their experiences differ onsite


-------------------------------------------- 
--how many users come from push?
--------------------------------------------
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
      end as reporting_channel
  , count(distinct visit_id) as visits
  , count(distinct user_id) as users
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30 and platform in ('boe')
group by all

select
  case 
      when v.top_channel like 'push_%' then v.top_channel
      else 'Other Traffic' 
      end as reporting_channel
  , count(distinct v.visit_id) as visits
  , count(distinct v.user_id) as users
from etsy-data-warehouse-prod.weblog.visits v 
where 
  _date >= current_date-30 
  and v.platform in ('boe')
group by all
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- what does engagement look like by push type, push vs not push (conversion rate, listing views, exit rate, engagement rate, types of engagement, acvv)?
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select
  case 
      when v.top_channel like 'push_%' then v.top_channel
      else 'Other Traffic' 
      end as reporting_channel
  , count(distinct v.visit_id) as visits
  , count(distinct v.user_id) as users
  , sum(v.converted) as conversions
  , count(distinct case when v.converted > 0 then v.visit_id end) as converted_visits
  , sum(v.total_gms) as total_gms
  , sum(v.total_gms)/sum(v.converted) as acvv
  , count(distinct case when timestamp_diff(v.end_datetime, v.start_datetime, second)> 300  then v.visit_id end) as visits_5_min
  , count(distinct case when  v.cart_adds > 0 or v.fav_item_count > 0 or v.fav_shop_count > 0 then v.visit_id end) as collected_visits
  , count(distinct case 
      when timestamp_diff(v.end_datetime, v.start_datetime,second)> 300 
      or v.cart_adds>0 or v.fav_item_count > 0 or v.fav_shop_count > 0
      or v.converted > 0
    then v.visit_id end) as engaged_visits
  , count(distinct lv.visit_id) as visits_with_listing_view
  , sum(lv.listing_views) as listing_views
from 
  etsy-data-warehouse-prod.weblog.visits v 
left join 
  (select visit_id, count(*) as listing_views from etsy-data-warehouse-prod.analytics.listing_views where _date >= current_date-30 group by all) lv using (visit_id)
where 
  _date >= current_date-30 
  and v.platform in ('boe')
group by all
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- reactivated users between push types, push vs not push 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- buyer segments and push engagements 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
