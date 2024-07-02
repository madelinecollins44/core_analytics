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

---------HOW DID USERS SEARCH IN THEIR FIRST VISIT
--base table 1: first visit
create or replace table etsy-data-warehouse-dev.madelinecollins.app_downloads_had_search_first_visit as ( -- stole this logic from sam 
 select
    b.browser_id,
    b.user_id,
    b.download_date,
    a.visit_id,
    a.query_raw,
    a.query,
    a.has_click,
    a.has_favorite,
    a.has_cart,
    a.has_purchase,
    a.max_page
  from `etsy-data-warehouse-prod.search.query_sessions_new` a 
  join `etsy-data-warehouse-dev.semanuele.browsers_of_interest` b
    on split(a.visit_id, ".")[offset(0)] = b.browser_id
    and a.visit_id=b.visit_id
  join 
    etsy-data-warehouse-dev.semanuele.boe_stickiness_all c
    on b.browser_id = c.browser_id
  where 
    a._date >= "2022-01-01" and a._date <= "2023-06-01"
    and a.platform in ('boe')
  group by all
);


  --pull all data associated w queries in visits on first day 
create or replace table etsy-data-warehouse-dev.madelinecollins.app_downloads_had_search_first_visit as ( -- stole this logic from sam 
  select
    b.browser_id,
    b.user_id,
    b.download_date,
    a._date,
    a.visit_id as search_visit_id,
    a.query_raw,
    a.query,
    a.has_click,
    a.has_favorite,
    a.has_cart,
    a.has_purchase,
    a.max_page
  from `etsy-data-warehouse-prod.search.query_sessions_new` a 
  join `etsy-data-warehouse-dev.semanuele.browsers_of_interest` b
    on split(a.visit_id, ".")[offset(0)] = b.browser_id
    and a._date = b.download_date -- only looks at visits from day of download 
  where a._date >= "2022-01-01" and a._date <= "2023-06-01"
);

--engagements around query 
--switch out base tables to get different levels of interaction 





