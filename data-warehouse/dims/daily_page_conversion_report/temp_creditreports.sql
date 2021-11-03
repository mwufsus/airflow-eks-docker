drop table if exists staging.temp_creditreport;

select applicationid, id
into staging.temp_creditreport
from spectrum.creditgenie_creditreport
where application_date = '{{params.date_string}}'
and environmentname not in ('staging','prod','master','main','freetool');
