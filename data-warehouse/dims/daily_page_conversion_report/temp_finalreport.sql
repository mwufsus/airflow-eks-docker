drop table if exists staging.staging_finalreport;

with result as (
    select distinct(staging.temp_apps.id),
                   staging.temp_borrowers.borrower_id,
                   staging.temp_borrowers.email,
                   staging.temp_apps.createdat,
                   staging.temp_apps.environmentname,
                   staging.temp_apps.lastviewedpage,
                   staging.temp_apps.lastcompletedpage,
                   staging.temp_apps.reasonfornotconnectingbank,
                   staging.temp_apps.kindofrelief,
                   staging.temp_apps.utmsource,
                   staging.temp_apps.utmcampaign,
                   staging.temp_apps.utmcontent,
                   staging.temp_apps.utmmedium,
                   staging.temp_apps.utmterm,
                   staging.temp_apps.router,
                   staging.temp_dmpenrollment.dmp_firstpaymentdate,
                   staging.temp_dmpenrollment.dmp_monthlypaymentamount,
                   staging.temp_dmpenrollment.dmp_agreement_signed,
                   staging.temp_dmpenrollment.dmp_term_in_months,
                   staging.temp_dmpenrollment.dmp_id,
                   staging.temp_plaiditems.plaiditemid,
                   CASE
                       WHEN staging.temp_creditreport.applicationid IS NOT NULL
                           THEN 1
                       ElSE 0
                       END as "credit_pulled",
                   CASE
                       WHEN staging.temp_debts.creditreportid IS NOT NULL and
                             staging.temp_debts.accounttype IN ('CC', 'CH', 'FE', 'FX', 'LC', 'US') and
                             staging.temp_debts.currentbalance > 0 and
                            staging.temp_debts.dateclosed is null
                           THEN 1
                       ElSE 0
                       END as "credit_we_can_help_with",
                   CASE
                       WHEN staging.temp_expenses.applicationid IS NOT NULL
                           THEN 1
                       ELSE 0
                       END as "expenses_finished",
                   CASE
                       when staging.temp_decisions.applicationid IS NOT NULL
                           AND isqualified = true
                           THEN 1
                       ELSE 0
                       END as "offer_generated",
                   CASE
                       when staging.temp_plaiditems.applicationid IS NOT NULL
                           THEN 1
                       ELSE 0
                       END as "connected_account",
                    CASE
                        when
                            staging.temp_decisions.applicationid IS NOT NULL
                            THEN 1
                        ELSE 0
                        END AS "completed_application",
                    CASE
                        when
                         staging.temp_dmpenrollment.dmp_id IS NOT NULL
                         THEN 1
                    ELSE 0
                    END as "dmp_enrolled"

    from staging.temp_apps
             left join staging.temp_dmpenrollment
                       on staging.temp_apps.id= staging.temp_dmpenrollment.dmpenrollmentapplicationid
             left join staging.temp_borrowers
                       on staging.temp_apps.id = staging.temp_borrowers.borrowerapplicationid
             left join staging.temp_creditreport
                       on staging.temp_apps.id = staging.temp_creditreport.applicationid
             left join staging.temp_debts
                       on staging.temp_creditreport.id = staging.temp_debts.creditreportid
             left join staging.temp_expenses
                       on staging.temp_apps.id = staging.temp_expenses.applicationid
             left join staging.temp_plaiditems
                       on staging.temp_apps.id = staging.temp_plaiditems.applicationid
             left join staging.temp_decisions
                       on staging.temp_apps.id = staging.temp_decisions.applicationid
             where staging.temp_apps.environmentname not in ('master', 'staging', 'prod','main','freetool')
),
     resultWithRowNum as (
         select r.*, row_number() over (partition by r.id order by credit_we_can_help_with desc ) as RowNum
         from result r)
select *
into staging.staging_finalreport
from resultWithRowNum
where RowNum = 1;