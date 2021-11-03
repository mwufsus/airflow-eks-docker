drop table if exists staging.temp_expenses;

select applicationid, createdat, updatedat
into staging.temp_expenses
from spectrum.creditgenie_expense
where application_date = '{{params.date_string}}'
and environmentname not in ('staging','prod','master','main','freetool');
