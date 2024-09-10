---code used to test monthly visits per user section in dashboard 
with agg as (
select 
  visit_id
  , _date
  , coalesce(cast(user_id as string), browser_id) as unique_id
  , platform
from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-365)
select
  date_trunc(_date, month) as month
  , platform
  , count(distinct visit_id)/ count(distinct unique_id) as monthly_visits_per_user
from agg
group by all 
-- month	platform	monthly_visits_per_user
-- 2024-09-01	desktop	1.6365790035536125
-- 2024-08-01	boe	6.3953948974960095
-- 2024-07-01	undefined	1.0540816764718512
-- 2024-06-01	mobile_web	1.8309229791051282
-- 2023-10-01	mobile_web	1.8904079031798036
-- 2024-08-01	mobile_web	1.8149018192352728
-- 2024-04-01	mobile_web	1.8711021451419596
-- 2024-09-01	boe	3.4061279340587514
-- 2024-01-01	boe	4.9906449632550283
-- 2024-04-01	undefined	1.0823885662458492
-- 2024-03-01	mobile_web	1.8557964459230407
-- 2024-05-01	mobile_web	1.8013284325991998
-- 2024-05-01	soe	34.277946848503589
-- 2024-05-01	undefined	1.1132758461680348
-- 2023-10-01	desktop	1.8782223913926119
-- 2024-05-01	desktop	1.5676016911147217
-- 2023-09-01	soe	24.749924582359132
-- 2024-02-01	desktop	1.8119489335971211
-- 2024-01-01	soe	21.11654245198153
-- 2024-07-01	soe	30.411356750739888
-- 2024-01-01	desktop	1.7317481941687183
-- 2024-07-01	boe	6.0093655600844631
-- 2023-10-01	undefined	1.0985391973383241
-- 2024-09-01	undefined	1.0517968629970529
-- 2024-04-01	boe	5.2021843797054244
-- 2024-06-01	desktop	1.5769049820592598
-- 2023-12-01	soe	31.914225086304587
-- 2024-06-01	undefined	1.0893566437621409
-- 2023-10-01	soe	34.050917637957788

--testing top channel 
with agg as (
select 
  visit_id
  -- , _date
  , coalesce(cast(user_id as string), browser_id) as unique_id
  , case 
    when top_channel in ('direct') then 'direct'
    when top_channel in ('internal') then 'internal'
    when top_channel like ('push%') then 'push'
    when top_channel in ('seo') then 'seo'
    when top_channel in ('direct') then 'direct'
    when top_channel in ('us_paid','intl_paid') and (second_channel like '%gpla' or second_channel like '%bing_plas')  then 'pla'
    else 'other'
    end as reporting_chanel
  from etsy-data-warehouse-prod.weblog.visits
where _date >= current_date-30 and platform in ('boe'))
select
  -- date_trunc(_date, month) as month
   reporting_chanel
  , count(distinct visit_id)/ count(distinct unique_id) as monthly_visits_per_user
from agg
group by all 
-- reporting_chanel	monthly_visits_per_user
-- pla	1.9137262533324542
-- direct	4.5639826084654187
-- seo	1.4675075348809856
-- other	2.1204281230219908
-- internal	3.0353855193354029
-- push	1.9876127480012802
