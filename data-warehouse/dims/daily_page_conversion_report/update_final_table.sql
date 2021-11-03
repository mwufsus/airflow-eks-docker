update daily_page_conversions
set reasonfornotconnectingbank = 'Connected Account'
where connected_account = 1;

update daily_page_conversions
set reasonfornotconnectingbank = 'Left Application at Bank Connection'
where plaiditemid is null
  and reasonfornotconnectingbank is null
  and connected_account = 0
  and completed_application = 0
and lastviewedpage ilike 'bankconnection%';

update daily_page_conversions
set reasonfornotconnectingbank = 'Completed Application - No Plaid Connection'
where plaiditemid is null
  and reasonfornotconnectingbank is null
  and connected_account = 0
  and completed_application = 1
and lastviewedpage ilike 'bankconnection%';

update daily_page_conversions
set completed_app_plaid = 1
where connected_account = 1
and completed_application = 1;

update daily_page_conversions
set completed_app_noplaid = 1
where plaiditemid is null
and completed_application = 1;

update daily_page_conversions
    set pricing_tier = 'Tier 3'
    where completed_application = 1
    and connected_account = 1
    and pricing_tier is null
    and createdat ilike '{{params.date_string}}%';

  update daily_page_conversions
    set pricing_tier = 'Tier 2'
    where credit_pulled = 1
    and pricing_tier is null
    and createdat ilike '{{params.date_string}}%';

  update daily_page_conversions
    set pricing_tier = 'Tier 1'
    where pricing_tier is null
    and createdat ilike '{{params.date_string}}%';

update daily_page_conversions
        set est_converted_date = convert_timezone('UTC', 'EST', createdat)
where est_converted_date is null;

update daily_page_conversions set applicant_time=dateadd(hour, zip.timezone, dpc.createdat)
from daily_page_conversions dpc, staging.staging_borrower borr, zips zip
where dpc.id = borr.borrowerapplicationid and borr.zipcode = zip.zip;

UPDATE
    daily_page_conversions
SET
    dmp_id = RAN.application_id
FROM
    daily_page_conversions SI
INNER JOIN
    dimensions_counselorgenie.f_counselorgenie_dmpenrollment RAN
ON
    SI.id = RAN.application_id;


update daily_page_conversions
set dmp_enrolled = 1
                       where dmp_id is not null;