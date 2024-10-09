--get first visit date from all mapped_user_ids
create or replace table etsy-data-warehouse-dev.madelinecollins.boe_downloads as (
select
  min(_date) as first_boe_visit,
      um.mapped_user_id,
    from `etsy-data-warehouse-prod.weblog.visits` v 
    left join `etsy-data-warehouse-prod.user_mart.user_mapping` um
        on v.user_id = um.user_id
    where platform in ('boe')
    and _date >= '2017-01-01'
  group by all 
);

-- get all boe visit dates 
create or replace table etsy-data-warehouse-dev.madelinecollins.boe_users_base as (
  with boe_visits_mapped as (
    select distinct
      _date,
      um.mapped_user_id,
    from `etsy-data-warehouse-prod.weblog.visits` v 
    left join `etsy-data-warehouse-prod.user_mart.user_mapping` um
        on v.user_id = um.user_id
    where platform = 'boe' and 
      v.user_id is not null and  --signed in 
      um.mapped_user_id is not null and 
      _date >= ('2022-01-01')
    group by all 
  )
    select distinct
      _date,
      mapped_user_id,
      first_boe_visit,
    from boe_visits_mapped
    left join etsy-data-warehouse-dev.madelinecollins.boe_downloads using (mapped_user_id)
);


create or replace table etsy-data-warehouse-dev.madelinecollins.maus_first_visit_date as (
with maus as (
  select 
    _date,
    first_boe_visit,
    count(distinct mapped_user_id) as mau
  from etsy-data-warehouse-dev.madelinecollins.boe_users_base 
  group by all
)
select  
  date_trunc(_date, month) as month,
  extract(month from first_boe_visit) as first_boe_visit_month,
  extract(year from first_boe_visit) as first_boe_visit_year,
  sum(mau) as mau,
from
  maus
group by all
);

------------------------------------------
--TESTING
------------------------------------------
-- select date_trunc(_date, month) as month,count(mapped_user_id) as visits from etsy-data-warehouse-dev.madelinecollins.boe_users_base
-- where mapped_user_id = 164613868
-- group by all
-- order by 1 desc

with agg as (select date_trunc(_date, month) as month,count(mapped_user_id) as visits from etsy-data-warehouse-dev.madelinecollins.boe_users_base
where mapped_user_id = 164613868
group by all
order by 1 desc)
select count(distinct month) from agg
--34

-- month	visits
-- 2024-10-01	4
-- 2024-09-01	24
-- 2024-08-01	29
-- 2024-07-01	23
-- 2024-06-01	27
-- 2024-05-01	18
-- 2024-04-01	26
-- 2024-03-01	30
-- 2024-02-01	28
-- 2024-01-01	30
-- 2023-12-01	25
-- 2023-11-01	23
-- 2023-10-01	21
-- 2023-09-01	22
-- 2023-08-01	23
-- 2023-07-01	28
-- 2023-06-01	21
-- 2023-05-01	29
-- 2023-04-01	22
-- 2023-03-01	20
-- 2023-02-01	27
-- 2023-01-01	11
-- 2022-12-01	22
-- 2022-11-01	24
-- 2022-10-01	26
-- 2022-09-01	15
-- 2022-08-01	23
-- 2022-07-01	31
-- 2022-06-01	20
-- 2022-05-01	24
-- 2022-04-01	28
-- 2022-03-01	30
-- 2022-02-01	23
-- 2022-01-01	22

with maus as (
  select 
    _date,
    first_boe_visit,
    count(distinct mapped_user_id) as mau
  from etsy-data-warehouse-dev.madelinecollins.boe_users_base 
  where mapped_user_id = 164613868
  group by all
)
, agg as (select  
  date_trunc(_date, month) as month,
  extract(month from first_boe_visit) as first_boe_visit_month,
  extract(year from first_boe_visit) as first_boe_visit_year,
  sum(mau) as mau,
from
  maus
group by all)
select
count(distinct month) from agg
--34
