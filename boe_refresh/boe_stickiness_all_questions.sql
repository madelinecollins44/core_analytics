-----HOW MANY USER, BROWSER COMBOS ARE THERE? 
select count(*) from etsy-data-warehouse-dev.semanuele.boe_stickiness_all 
--36258055

--how many buyers (user_id + browser_id combo) created an account prior to download_date
with agg as (
select
  a.user_id
  , a.browser_id
  -- , concat(a.user_id,'|' ,a.browser_id) as concat
  , a.download_date
  , date(timestamp_seconds(b.join_date)) as recipient_join_date
from 
  etsy-data-warehouse-dev.semanuele.boe_stickiness_all a
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile b 
    on a.user_id = b.mapped_user_id
where date(timestamp_seconds(b.join_date))  < a.download_date
)
select 
count(*)
from agg 
--9892131

--SELECT 9892131/36258055 = 0.27 

--different joining moments 
--how many buyers (user_id + browser_id combo) created an account prior to download_date
with agg as (
select
  a.user_id
  , a.browser_id
  , a.download_date
  , date(timestamp_seconds(b.join_date)) as recipient_join_date
  , case when date(timestamp_seconds(b.join_date)) < a.download_date then 1 else 0 end as join_before_download
  , case when date(timestamp_seconds(b.join_date)) = a.download_date then 1 else 0 end as join_with_download
  , case when date(timestamp_seconds(b.join_date)) > a.download_date then 1 else 0 end as join_after_download
  , case when date(timestamp_seconds(b.join_date)) is null then 1 else 0 end as no_account
from 
  etsy-data-warehouse-dev.semanuele.boe_stickiness_all a
left join 
  etsy-data-warehouse-prod.user_mart.mapped_user_profile b 
    on a.user_id = b.mapped_user_id
-- where date(timestamp_seconds(b.join_date)) >= a.download_date
group by all 
)
select sum(join_before_download) as join_before_download, sum(join_with_download) as join_with_download, sum(join_after_download) as join_after_download, sum(no_account) as no_account from agg 

--how did users search in their first visit
create or replace table etsy-data-warehouse-dev.madelinecollins.app_downloads_had_search_first_visit as (
select 
  a.user_id
  , a.browser_id
  , a.download_date
  , b._date as visit_date 
  , b.visit_id
from etsy-data-warehouse-dev.semanuele.boe_stickiness_all a 
inner join etsy-data-warehouse-prod.weblog.visits b
  on (a.user_id=b.user_id or a.user_id is null and b.user_id is null)
  and a.browser_id=b.browser_id
  and a.download_date=b._date
where 
  a.had_search_first_visit = 1
  and b._date >= "2022-01-01"
  and b.platform in ('boe')
); 
