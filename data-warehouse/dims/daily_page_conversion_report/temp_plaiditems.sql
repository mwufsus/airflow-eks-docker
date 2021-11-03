drop table if exists staging.temp_plaiditems;

select applicationid, id as plaiditemid
into staging.temp_plaiditems
from spectrum.credtigenie_plaiditem
where application_date = '{{params.date_string}}'
and environmentname not in ('staging','prod','master','main','freetool');
