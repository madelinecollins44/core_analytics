declare last_date date;

-- drop table if exists `etsy-data-warehouse-dev.rollups.boe_churn_segmentation`;

create table if not exists `etsy-data-warehouse-dev.rollups.boe_churn_segmentation`  (
    _date date
    , users_visit_in_last_year int64
    , user_visit_in_same_day int64
    , user_visit_boe_in_last_week int64
    , user_visit_boe_in_two_weeks int64
    , user_visit_boe_in_three_weeks int64
    , user_visit_boe_in_last_month int64
    , user_visit_boe_in_three_months int64
    , user_visit_boe_in_six_months int64
    , user_visit_boe_in_year int64
); 

set last_date = (select max(_date) from `etsy-data-warehouse-dev.rollups.boe_churn_segmentation`);
 if last_date is null then set last_date = (select min(_date)-1 from `etsy-data-warehouse-prod.buyer360.buyer_ltd`);
 end if;

insert into `etsy-data-warehouse-dev.rollups.boe_churn_segmentation` (
select
  _date
  , count(distinct case when last_visit_date >= current_date-365 then mapped_user_id end) as users_visit_in_last_year
  , count(distinct case when last_boe_visit_date = _date then mapped_user_id end) as user_visit_in_same_day
  , count(distinct case when last_boe_visit_date <= _date - 6 then mapped_user_id end) as user_visit_boe_in_last_week
  , count(distinct case when last_boe_visit_date <= _date - 14 then mapped_user_id end) as user_visit_boe_in_two_weeks
  , count(distinct case when last_boe_visit_date <= _date - 20 then mapped_user_id end) as user_visit_boe_in_three_weeks
  , count(distinct case when last_boe_visit_date <= _date - 29 then mapped_user_id end) as user_visit_boe_in_last_month
  , count(distinct case when last_boe_visit_date <= _date - 89 then mapped_user_id end) as user_visit_boe_in_three_months
  , count(distinct case when last_boe_visit_date <= _date - 179 then mapped_user_id end) as user_visit_boe_in_six_months
  , count(distinct case when last_boe_visit_date <= _date - 364 then mapped_user_id end) as user_visit_boe_in_year

from 
  etsy-data-warehouse-prod.buyer360.buyer_ltd
where 
  _date >= last_date
  and last_visit_date >= current_date-365 -- excludes any user that has not visited in a year 
group by all
); 
