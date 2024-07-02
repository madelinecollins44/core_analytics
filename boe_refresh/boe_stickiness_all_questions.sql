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
    a.query_session_id,
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

--base table 2: pull all data associated w queries in visits on first day 
create or replace table etsy-data-warehouse-dev.madelinecollins.app_downloads_had_search_first_visit_whole_day as ( -- stole this logic from sam 
 select
    b.browser_id,
    b.user_id,
    b.download_date,
    a.visit_id,
    a.query_raw,
    a.query,
    a.query_session_id,
    a.has_click,
    a.has_favorite,
    a.has_cart,
    a.has_purchase,
    a.max_page
  from 
    (select browser_id, user_id, download_date from etsy-data-warehouse-dev.semanuele.boe_stickiness_all where had_search_first_visit =1) b
  inner join 
    `etsy-data-warehouse-prod.search.query_sessions_new` a 
    on split(a.visit_id, ".")[offset(0)] = b.browser_id
    and b.download_date=a._date
  where 
    a._date >= "2022-01-01" and a._date <= "2023-06-01"
    and a.platform in ('boe')
  group by all
);


--engagements around query 
--switch out base tables to get different levels of interaction 
--most common words 
with words as (
select 
  word
 , visit_id
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
limit 50

--signed in vs signed out,total engagement metrics, pagnation
select
  count(case when user_id is null then query_session_id end) as signed_out_query_sessions
  , count(case when user_id is not null then query_session_id end) as signed_in_query_sessions
  , count(query_session_id) as query_sessions
  , sum(has_click) as clicks
  , sum(has_favorite) as favorites
  , sum(has_cart) as carts
  , sum(has_purchase) as purchases
  , avg(max_page) as pagnation
  from etsy-data-warehouse-dev.madelinecollins.app_downloads_had_search_first_visit

  --overall metrics 
select
count(query)
, sum(has_click)
, sum(has_favorite) as favorites
, sum(has_cart) as carts
, sum(has_purchase) as purchases
, avg(max_page) as pagnation
  from `etsy-data-warehouse-prod.search.query_sessions_new` a 
where _date >= current_date-730

--type of queries
select
b.inference.label 
, count(query_session_id)
 from etsy-data-warehouse-dev.madelinecollins.app_downloads_had_search_first_visit a
 join `etsy-data-warehouse-prod.arizona.query_intent_labels` b  using (query_raw)
 group by all 

--bins 
select 
b.bin
, count(a.query_session_id) as sessions
 from etsy-data-warehouse-dev.madelinecollins.app_downloads_had_search_first_visit a
 join `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` b  using (query_raw)
 group by all

----FUNNEL VIEW 
--overall 
with overall as (
select
  b.user_id
  , b.browser_id
  , count(distinct b.visit_id) as boe_visits
  , count(distinct a.visit_id) as search_visits
  , sum(has_click) as clicks
  , sum(has_favorite) as favorites
  , sum(has_cart) as carts
  , sum(has_purchase) as purchases
  , avg(max_page) as pagnation
from 
  etsy-data-warehouse-prod.weblog.visits b 
left join 
  (select 
    visit_id
    , sum(has_click) as has_click
    , sum(has_favorite) as has_favorite
    , sum(has_cart) as has_cart
    , sum(has_purchase) as has_purchase
    , avg(max_page) as max_page
  from 
    `etsy-data-warehouse-prod.search.query_sessions_new` 
  where _date >= "2022-01-01" and _date <= "2023-06-01"
  group by all ) a -- need to break this out to include nulls from this table 
      using (visit_id)
where 
  (b._date >= "2022-01-01" and b._date <= "2023-06-01")
  and b.platform in ('boe')
  group by all
)
--1.7% of browsers have more than 1 user, exclude these edge cases 
, to_remove as (
    select browser_id, count(distinct user_id) as users from overall group by all order by 2 desc
  )
select 
  count(browser_id) as browsers
  , count(case when search_visits > 0 then browser_id end) as browsers_with_search
  , count(case when clicks > 0 then browser_id end) as browsers_with_clicks
  , count(case when favorites > 0 then browser_id end) as browsers_with_favorites
  , count(case when carts > 0 then browser_id end) as browsers_with_carts
  , count(case when purchases > 0 then browser_id end) as browsers_with_purchases
  , avg(pagnation) as pagnation
from overall a
left join to_remove r 
  using(browser_id)
where r.browser_id is null

--first time visits 
with first_visit as (
select
  a.user_id
  , a.browser_id
  , case when b.browser_id is not null then 1 else 0 end as searched
  , sum(has_click) as clicks
  , sum(has_favorite) as favorites
  , sum(has_cart) as carts
  , sum(has_purchase) as purchases
  , avg(max_page) as pagnation
from 
  etsy-data-warehouse-dev.semanuele.boe_stickiness_all a 
left join 
  etsy-data-warehouse-dev.madelinecollins.app_downloads_had_search_first_visit b
    using (user_id, browser_id)
group by all 
)
select 
    count(browser_id) as browsers
  , count(case when searched > 0 then browser_id end) as browsers_with_search
  , count(case when clicks > 0 then browser_id end) as browsers_with_clicks
  , count(case when favorites > 0 then browser_id end) as browsers_with_favorites
  , count(case when carts > 0 then browser_id end) as browsers_with_carts
  , count(case when purchases > 0 then browser_id end) as browsers_with_purchases
  , avg(pagnation) as pagnation
from first_visit
group by all 


