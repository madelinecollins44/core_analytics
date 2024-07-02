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
--make table with visit_ids on day of download for user + browser
-- create or replace table etsy-data-warehouse-dev.madelinecollins.app_downloads_had_search_first_visit as (
-- select 
--   a.user_id
--   , a.browser_id
--   , a.download_date
--   , b._date as visit_date 
--   , b.visit_id
-- from etsy-data-warehouse-dev.semanuele.boe_stickiness_all a 
-- inner join etsy-data-warehouse-prod.weblog.visits b
--   on (a.user_id=b.user_id or a.user_id is null and b.user_id is null)
--   and a.browser_id=b.browser_id
--   and a.download_date=b._date
-- where 
--   a.had_search_first_visit = 1
--   and b._date >= "2022-01-01"
--   and b.platform in ('boe')
--   and a.download_date=b._date
-- ); 

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

--get engagmenet around query 
create or replace table etsy-data-warehouse-dev.madelinecollins.boe_stickiness_had_search_first_visit_queries as (
select 
  a.user_id
  , a.browser_id
  , a.visit_id
  , b.query
  , c.bin
  , b.has_click
  , b.has_favorite
  , b.has_cart	
  , b.has_purchase
from 
  etsy-data-warehouse-dev.madelinecollins.app_downloads_had_search_first_visit a
left join 
  etsy-data-warehouse-prod.search.query_sessions_new b
    using (visit_id)
left join etsy-data-warehouse-prod.search.query_bins c
  on b.query_raw=c.query_raw
where 
  b.platform in ('boe')
  and b._date >= '2022-01-01'
);


--most common search queries in first day visits 
with words as (
select 
  word
 , search_visit_id as visit_id
 , user_id
 , browser_id
from etsy-data-warehouse-dev.madelinecollins.app_downloads_had_search_first_visit,
unnest(split(query, ' ')) as word
)
select
  word
  , count(visit_id) as searches 
  , count(distinct user_id) as users
  , count(distinct browser_id) as browsers
from words
group by all order by 2 desc 






