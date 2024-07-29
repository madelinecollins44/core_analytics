------------------------------------------------------------
--using first download date
------------------------------------------------------------

------------------------------------------------------------
--using marketings app download tables
------------------------------------------------------------
with all_downloads as (
--organic installs 
select
  Country_Code
  , date(Event_Time) as download_date
  , platform
  , advertising_id 
  , 'organic' as download_type
from
  etsy-data-warehouse-prod.marketing.appsflyer_organic_installs -- organic app 
union all
-- paid downloads 
select
  Country_Code
  , date(Event_Time) as download_date
  , platform
  , advertising_id 
  , 'paid' as download_type
from
  etsy-data-warehouse-prod.marketing.appsflyer_paid_installs 
) 
select 
  downloads.country_code
  , downloads.download_date
  , downloads.platform
  , downloads.download_type
  , segments.buyer_segment
  , downloads.advertising_id
from 
  all_downloads downloads
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile segments 
    on downloads.advertising_id=cast(segments.mapped_user_id as string)
