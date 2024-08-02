with b360 as (
select
  _date
  , mapped_user_id
  , last_boe_visit_date
  -- , last_visit_date
  -- , case when last_visit_date > last_boe_visit_date then 'last_visit_not_boe' else null end
from 
  etsy-data-warehouse-prod.buyer360.buyer_ltd
where 
  _date >= current_date-5
  and last_visit_date <= current_date-365 -- excludes any user that has not visited in a year 
)
select
  a._date
  , count(distinct case when a._date between date_sub(a._date, interval 1 year) and a._date then a.visit_id end) AS visits_last_year
  , count(distinct case when b.last_boe_visit_date=a._date then mapped_user_id end) as users_visit_today
  , count(distinct case when b.last_boe_visit_date between date_sub(a._date, interval 1 week) and a._date then mapped_user_id end) as users_visit_one_week
  , count(distinct case when b.last_boe_visit_date between date_sub(a._date, interval 2 week) and a._date then mapped_user_id end) as users_visit_two_weeks
  , count(distinct case when b.last_boe_visit_date between date_sub(a._date, interval 3 week) and a._date then mapped_user_id end) as users_visit_three_week
  , count(distinct case when b.last_boe_visit_date between date_sub(a._date, interval 1 month) and a._date then mapped_user_id end) as users_visit_one_month
  , count(distinct case when b.last_boe_visit_date between date_sub(a._date, interval 3 month) and a._date then mapped_user_id end) as users_visit_three_months
  , count(distinct case when b.last_boe_visit_date between date_sub(a._date, interval 6 month) and a._date then mapped_user_id end) as users_visit_six_months
  , count(distinct case when b.last_boe_visit_date between date_sub(a._date, interval 1 year) and a._date then mapped_user_id end) as users_visit_one_year
from 
  etsy-data-warehouse-prod.weblog.visits a
left join 
  b360 b 
    on a._date=b._date
where 
  a._date >= current_date-5
  and a.platform in ('boe')
group by all
