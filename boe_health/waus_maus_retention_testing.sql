agg as (
  select
  nw.week,
  -- vi.buyer_segment, 
  vi.top_channel,
  vi.browser_platform,
  vi.region,
  vi.signed_in,
  nw.mapped_user_id,
  count(nw.mapped_user_id) as waus, 
  count(case when nw.next_visit_week = date_add(week, interval 1 week) then mapped_user_id end) as retained,
  count(case when nw.next_visit_week = date_add(week, interval 1 week) then mapped_user_id end) / count(*) as pct_retained,
  sum(gms) as gms
from 
  next_visit_week nw 
left join 
  common_visit_info vi
    using (mapped_user_id)
group by all 
)
select mapped_user_id, sum(retained) from agg group by all having sum(retained) = 5 order by 2 desc
-- 714770200, 12 weekly retains
-- 802447184, 12
-- 5822068, 12
-- 23997, 5
-- 56606, 5


-----testing to make sure there are the right amount of entries
with waus as (
  select 
    date_trunc(_date, week) as week,
    -- buyer_segment,
    top_channel,
    browser_platform,
    region,
    case when v.user_id is not null then 1 else 0 end as signed_in,
    b.mapped_user_id,
    count(distinct v.visit_id) as visits,
    sum(v.total_gms) as gms,
  from 
    etsy-data-warehouse-prod.weblog.visits v 
  left join  
    etsy-data-warehouse-prod.user_mart.user_mapping b 
      on v.user_id=b.user_id 
  -- left join 
  --   buyer_segment s 
  --     on b.mapped_user_id=s.mapped_user_id
  --     and v_date= s.as_of_date
  where platform = "boe"
    and _date >= "2024-06-01"
    and _date != "2024-02-29"
  group by all
  )
  select week, lead(week) over (partition by mapped_user_id order by week asc), case when lead(week) over (partition by mapped_user_id order by week asc) = date_add(week, interval 1 week) then 1 else 0 end as retained from waus where mapped_user_id in (714770200) order by week desc
----714770200 this works --> even with diuped weeks, retain only count once 
-- -- week	f0_	retained
-- -- 2024-08-18	2024-08-18	0
-- -- 2024-08-18		0
-- -- 2024-08-11	2024-08-11	0
-- -- 2024-08-11	2024-08-11	0
-- -- 2024-08-11	2024-08-18	1
-- -- 2024-08-04	2024-08-11	1
-- -- 2024-07-28	2024-08-04	1
-- -- 2024-07-21	2024-07-28	1
-- -- 2024-07-14	2024-07-21	1
-- -- 2024-07-07	2024-07-14	1
-- -- 2024-06-30	2024-06-30	0
-- -- 2024-06-30	2024-07-07	1
-- -- 2024-06-23	2024-06-30	1
-- -- 2024-06-16	2024-06-23	1
-- -- 2024-06-09	2024-06-09	0
-- -- 2024-06-09	2024-06-16	1
-- -- 2024-06-02	2024-06-09	1
-- -- 2024-05-26	2024-06-02	1

----23997 this works --> even with diuped weeks, retain only count once 
-- -- week	f0_	retained
-- -- 2024-08-18	2024-08-18	0
-- -- 2024-08-18		0
-- -- 2024-07-28	2024-07-28	0
-- -- 2024-07-28	2024-07-28	0
-- -- 2024-07-28	2024-07-28	0
-- -- 2024-07-28	2024-08-18	0
-- -- 2024-07-21	2024-07-21	0
-- -- 2024-07-21	2024-07-21	0
-- -- 2024-07-21	2024-07-21	0
-- -- 2024-07-21	2024-07-28	1
-- -- 2024-07-14	2024-07-21	1
-- -- 2024-07-07	2024-07-07	0
-- -- 2024-07-07	2024-07-14	1
-- -- 2024-06-23	2024-07-07	0
-- -- 2024-06-16	2024-06-16	0
-- -- 2024-06-16	2024-06-16	0
-- -- 2024-06-16	2024-06-16	0
-- -- 2024-06-16	2024-06-23	1
-- -- 2024-06-09	2024-06-16	1


---test to see if top characteristcs are true
select 
    date_trunc(v._date, week) as week,
    -- buyer_segment,
    top_channel,
    browser_platform,
    region,
    -- case when v.user_id is not null then 1 else 0 end as signed_in,
    b.mapped_user_id,
    count(distinct v.visit_id) as visits,
    sum(v.total_gms) as gms,
  from 
    etsy-data-warehouse-prod.weblog.visits v 
  left join  
    etsy-data-warehouse-prod.user_mart.user_mapping b 
      on v.user_id=b.user_id 
  -- left join 
  --   buyer_segment s --grabs buyer_segment at beginning of week
  --     on b.mapped_user_id=s.mapped_user_id
  --     and v._date=s._date
  where platform = "boe"
    and v._date >= "2024-05-01"
    and v._date != "2024-02-29"
    and b.mapped_user_id=185486287
  group by all
-- week	top_channel	browser_platform	region	mapped_user_id	visits	gms
-- 2024-06-02	direct	iOS	US	185486287	1	0
-- 2024-05-19	direct	iOS	US	185486287	1	0
-- 2024-07-21	direct	iOS	US	185486287	2	0
-- 2024-05-05	direct	iOS	US	185486287	4	37.98
-- 2024-05-12	internal	iOS	US	185486287	1	0
-- 2024-05-05	internal	iOS	US	185486287	1	0
-- 2024-05-12	push_lifecycle	iOS	US	185486287	1	0
-- 2024-05-12	direct	iOS	US	185486287	6	0

select * from etsy-bigquery-adhoc-prod._script00e252c815d08c127eb2cb1ed5bb7153033dd758.agg where mapped_user_id = 185486287
-- mapped_user_id	top_channel	browser_platform	region	buyer_segment
-- 185486287	direct	iOS	US	Repeat
