select count(distinct mapped_user_id)
from 
  etsy-data-warehouse-prod.buyer360.buyer_ltd
where 
  -- last_visit_date >= _date - 29 -- lats visit is within ast 30 days
  last_boe_visit_date >= _date - 29 -- last boe visit is not within last 30 days 


  --not visit boe in last 30 days but have visited another platofrm: 2380985
  -- viist boe in last 30 days: 
