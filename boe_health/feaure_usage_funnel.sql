--owner: madelinecollins@etsy.com
--owner_team: product-asf@etsy.com
--description: a rollup for measuring engagement with the gift mode discovery experience

BEGIN

declare last_date date;

-- drop table if exists `etsy-data-warehouse-dev.rollups.boe_tab_engagement`;

set last_date = (select max(_date) from `etsy-data-warehouse-dev.rollups.boe_tab_engagement`);
 if last_date is null then set last_date = (select min(_date)-1 from `etsy-data-warehouse-prod.weblog.events`);
 end if;

create table if not exists `etsy-data-warehouse-dev.rollups.boe_tab_engagement` (
  _date DATE
  , browser_platform STRING
  , region STRING
  , buyer_segment STRING
  , signed_in int64
  , total_visits int64
  , home_visits int64
  , deals_visits int64
  , gift_mode_home_visits int64
  , favorites_visits int64
  , cart_view_visits int64
  , search_visits int64
  , one_plus_tabs int64
  , two_plus_tabs int64
  , three_plus_tabs int64
  , four_plus_tabs int64
  , five_plus_tabs int64
  , six_plus_tabs int64
);

insert into `etsy-data-warehouse-dev.rollups.boe_tab_engagement` (
with small_events as (
select
a.event_type,
a.visit_id,
a._date,
c.buyer_segment,
b.mapped_user_id,
case when a.user_id is not null then 1 else 0 end as signed_in
from
  `etsy-data-warehouse-prod.weblog.events` a
inner join  
  `etsy-data-warehouse-prod.weblog.visits` d using (visit_id)
left join
  etsy-data-warehouse-prod.user_mart.user_mapping b 
    on a.user_id=b.user_id
left join
  etsy-data-warehouse-prod.rollups.buyer_segmentation_vw c
    on b.mapped_user_id=c.mapped_user_id
    and a._date=c.as_of_date
where 
  event_type in ('homescreen','deals_tab_delivered','app_deals','gift_mode_home','favorites_and_lists','cart_view','browselistings')
  and d.platform in ('boe')
  and d._date is not null
)
, count_events as (
select
  visit_id
  , _date
  , buyer_segment
  , signed_in
  , max(case when e.event_type in ('homescreen') then 1 else 0 end) as saw_home
  , max(case when e.event_type in ('deals_tab_delivered','app_deals') then 1 else 0 end) as saw_deals
  , max(case when e.event_type in ('gift_mode_home') then 1 else 0 end) as saw_gift_mode_home
  , max(case when e.event_type in ('favorites_and_lists') then 1 else 0 end) as saw_favorites_and_lists
  , max(case when e.event_type in ('cart_view') then 1 else 0 end) as saw_cart_view
  , max(case when e.event_type in ('browselistings') then 1 else 0 end) as saw_browselistings
from small_events e
group by all
)
select
  v._date
  , v.browser_platform
  , v.region
  , buyer_segment
  , signed_in
  , count(distinct v.visit_id) as total_visits
  , count(distinct case when saw_home > 0 then e.visit_id end) as home_visits
  , count(distinct case when saw_deals > 0 then e.visit_id end) as deals_visits
  , count(distinct case when saw_gift_mode_home > 0 then e.visit_id end) as gift_mode_home_visits
  , count(distinct case when saw_favorites_and_lists > 0 then e.visit_id end) as favorites_visits
  , count(distinct case when saw_cart_view > 0 then e.visit_id end) as cart_view_visits
  , count(distinct case when saw_browselistings > 0 then e.visit_id end) as search_visits
  , count(distinct case when saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings > 1 then e.visit_id end) as one_plus_tabs
  , count(distinct case when saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings > 2 then e.visit_id end) as two_plus_tabs
  , count(distinct case when saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings > 3 then e.visit_id end) as three_plus_tabs
  , count(distinct case when saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings  > 4 then e.visit_id end) as four_plus_tabs
  , count(distinct case when saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings > 5 then e.visit_id end) as five_plus_tabs
    , count(distinct case when saw_home + saw_deals+ saw_gift_mode_home+ saw_favorites_and_lists+ saw_cart_view+saw_browselistings  > 6 then e.visit_id end) as six_plus_tabs
from
  `etsy-data-warehouse-prod.weblog.recent_visits` v
left join -- just want to visit info from these sources
  count_events e using(visit_id, _date)
where
  v._date >= current_date-30
  and v.platform = 'boe'
  and v.event_source in ('ios','android')
group by all
);

END
