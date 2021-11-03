select est_converted_date as application_start_date,
       borrower_id,
       email,
       pricing_tier,
       environmentname,
       router,
       completed_application,
       utmsource,
       decision_generated,
       dmp_enrolled
from public.daily_page_conversions
where createdat ilike '{{params.date_string}}%';