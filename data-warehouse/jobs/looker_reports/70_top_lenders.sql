DROP TABLE IF EXISTS staging.single_credit_files;
DROP TABLE IF EXISTS looker_reporting_counselorgenie.70_top_lenders_topcharts;
TRUNCATE TABLE IF EXISTS looker_reporting_counselorgenie.70_top_lenders_bottomcharts;


SELECT
     *
INTO staging.single_credit_files
FROM
 (
     SELECT *,
            ROW_NUMBER() OVER (PARTITION BY accountnumber ORDER BY createdat DESC) AS rn
            FROM staging.staging_credit
     WHERE currentbalance > 0
     and portfoliotype = 'revolving'
     and dateclosed is null
 )
 WHERE rn = 1
 AND environmentname not in ('master','main','staging','prod');

UPDATE staging.single_credit_files
set lendername = 'WELLS FARGO'
where lendername ilike 'WF/%';

UPDATE staging.single_credit_files
set lendername = 'SYNCHRONY'
WHERE lendername ilike 'SYNC%';

UPDATE staging.single_credit_files
set lendername = 'CAPITAL ONE'
WHERE lendername ilike 'CAP1%';

UPDATE staging.single_credit_files
set lendername = 'WELLS FARGO'
WHERE lendername ilike 'WFDS%';

UPDATE staging.single_credit_files
set lendername = 'COMMENITY BANK'
WHERE lendername ilike 'CB/%';


UPDATE staging.single_credit_files
set lendername = 'COMMENITY BANK'
WHERE lendername = 'CB/VICSCRT';

UPDATE staging.single_credit_files
set lendername = 'JP Morgan CHASE'
where lendername = 'JPMCB CARD';

UPDATE credit_files
set lendername = 'BARCLAYS CARD'
where lendername = 'BRCLYSBANKDE';

UPDATE staging.single_credit_files
set lendername = 'TD BANK'
where lendername = 'TARGET/TD';

UPDATE staging.single_credit_files
set lendername = 'TD BANK'
where lendername = 'NORDSTM/TD';

UPDATE staging.single_credit_files
set lendername = 'CITIBANK'
where lendername = 'CITI';

UPDATE staging.single_credit_files
set lendername = 'DISCOVER'
where lendername = 'DISCOVERBANK';

UPDATE staging.single_credit_files
set lendername = 'CAPITAL ONE'
where lendername = 'KOHLS/CAPONE';

UPDATE staging.single_credit_files
set lendername = 'TD BANK'
where lendername = 'TD BANK N.A.';

UPDATE staging.single_credit_files
set lendername = 'CITIBANK'
where lendername ilike '%CBNA%';

UPDATE staging.single_credit_files
set lendername = 'COMMENITY BANK'
where lendername ilike '%CCB%';

UPDATE staging.single_credit_files
set lendername = 'AMERICAN EXPRESS'
where lendername ilike '%AMEX%';

UPDATE staging.single_credit_files
set lendername = 'BARCLAYS BANK'
where lendername ilike '%BRCLYSBANKDE%';

UPDATE staging.single_credit_files
set lendername = 'The Bank of Missouri'
where lendername ilike '%TBOM%';

UPDATE staging.single_credit_files
set lendername = 'BANK OF AMERICA'
where lendername = 'BK OF AMER';

UPDATE staging.single_credit_files
set lendername = 'BANK OF AMERICA'
where lendername = 'BANKAMERICA';

update staging.single_credit_files
set lendername = 'Citibank North America'
where lendername = 'SEARS/CBNA';

with records as
         (
             select lendername, cnt, row_number() over (order by cnt desc) as rn
             from (
                      select lendername, count(distinct creditreportid) as cnt
                      from staging.single_credit_files
                      group by lendername) x
         )
select coalesce(b.lendername, 'Others') as lendername,
       sum(a.cnt)
into reporting_counselorgenie.70_top_lenders_topcharts
from records a
left outer join records b on a.lendername = b.lendername
and b.rn<=50
group by b.lendername;

UPDATE TABLE looker_reporting_counselorgenie.70_top_lenders_bottomcharts
SET 