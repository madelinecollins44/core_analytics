--get browsers/ users with listing view or search in first week
select browser_id, user_id, download_date, had_listing_view_w_7d, had_search_w_7d from etsy-data-warehouse-dev.semanuele.boe_onboarding where had_listing_view_w_7d >0 or had_search_w_7d >0
