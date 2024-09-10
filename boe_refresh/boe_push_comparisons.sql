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

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- what does engagement look like by push type, push vs not push (conversion rate, listing views, exit rate, engagement rate, types of engagement, acvv)?
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- reactivated users between push types, push vs not push 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- buyer segments and push engagements 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
