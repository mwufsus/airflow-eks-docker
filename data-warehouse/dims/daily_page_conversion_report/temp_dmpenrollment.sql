drop table if exists staging.temp_dmpenrollment;

select active as dmp_flag,
       firstpaymentdate as dmp_firstpaymentdate,
       monthlypaymentamount as dmp_monthlypaymentamount,
       datesigned as dmp_agreement_signed,
       terminmonths as dmp_term_in_months,
       id as dmp_id,
       dmpenrollmentapplicationid
into staging.temp_dmpenrollment
from spectrum.creditgenie_dmpenrollment
where application_date = '{{params.date_string}}'
and environmentname not in ('staging','prod','master','main','freetool');

