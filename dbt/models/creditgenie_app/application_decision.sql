WITH source_data AS (
SELECT
    rdata."id" :: varchar(64) as decision_id,
    rdata."customerId" :: varchar(64) as customer_id,
    rdata."numberOfAccountOwners" :: varchar(24) as numberOfAccountOwners,
    rdata."averageDaysBetweenIncomeTransactions" :: varchar(24) as daysbetweenincome,
    rdata."isEligible" :: varchar(24) as is_eligible,
    rdata."averageCashflowOnPayday" :: float as averageCashflowOnPayday,
    rdata."averageIncome" :: float as averageIncome,
    rdata."currentBalance" :: float as currentBalance,
    rdata."accountSubType" :: varchar(54) as accounttype,
    rdata."nextPayday" :: varchar(24) as nextPayday,
    rdata."accountAgeInMonths" :: varchar(24) as accountAgeInMonths,
    rdata."incomeTransactionIds" :: varchar(24) as incomeTransactionIds,
    rdata."environmentname" :: varchar(24) as environmentname,
    rdata."createdAt" :: timestamp as createdat,
    rdata."updatedAt" :: timestamp as updatedat,
    getdate() as record_created_at
FROM
    (
        select
            *,
            ROW_NUMBER() OVER (
                PARTITION BY rdata."id" :: varchar(64)
                ORDER BY
                    rdata."createdAt" :: timestamp DESC
            ) AS rn
        from
            {{ source('data_staging', 'creditgenie_app_decision')}}
    )
WHERE
    rn = 1
        AND rdata."environmentName"::varchar(24) in ('prod')
)
SELECT *
FROM source_data