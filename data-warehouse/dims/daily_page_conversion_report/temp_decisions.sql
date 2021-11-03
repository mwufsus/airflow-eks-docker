drop table if exists staging.temp_decisions;

select applicationid, isqualified, id, createdat
into staging.temp_decisions
from spectrum.creditgenie_decision
where application_date = '{{params.date_string}}'
and environmentname not in ('staging','prod','master','main','freetool');
