---------------------------------------------------------------------------------------------------
--looking at table on browser level, making sure its doing what we want
---------------------------------------------------------------------------------------------------
-- select * from etsy-data-warehouse-dev.madelinecollins.boe_onboarding_funnel_events where browser_id="76A49588B1B74630B1C0259E552D" order by screen asc
select browser_id, event_name, count(*) from etsy-data-warehouse-dev.madelinecollins.boe_onboarding_funnel_events group by all
----all unique
