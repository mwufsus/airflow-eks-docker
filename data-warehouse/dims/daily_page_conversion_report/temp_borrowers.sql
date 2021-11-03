drop table if exists staging.temp_borrowers;

select id as borrower_id, borrowerapplicationid, email
into staging.temp_borrowers
from spectrum.creditgenie_borrower
where application_date = '{{params.date_string}}'
and environmentname not in ('staging','prod','master','main','freetool');
