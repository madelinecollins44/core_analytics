select 
count(case when user_id is null and browser_id is not null then browser_id end)/ count(*) from etsy-data-warehouse-dev.semanuele.boe_onboarding
--- about 27.7% of these dont have user_ids
--browser_ids only
------HkYtNGFFSNa5i2tTYGnPkg
------907230C03F8B42EBA2FF586D0DF7
------32BAF1CB715F4204B12D3365C241
------0yvzcIC1f6mxB5V_K9fbPJc_UjJ8

-------------
_________________
--find associated visit_ids with browsers/user_ids
  
  
--get browsers/ users with listing view or search in first week
select browser_id, user_id, download_date, had_listing_view_w_7d, had_search_w_7d from etsy-data-warehouse-dev.semanuele.boe_onboarding where had_listing_view_w_7d >0 or had_search_w_7d >0
