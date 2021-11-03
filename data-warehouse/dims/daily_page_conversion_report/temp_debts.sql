drop table if exists staging.temp_debts;

select creditreportid, accounttype, currentbalance, dateclosed
into staging.temp_debts
from spectrum.tu_debt
where application_date = '{{params.date_string}}'
and environmentname not in ('staging','prod','master','main','freetool');
