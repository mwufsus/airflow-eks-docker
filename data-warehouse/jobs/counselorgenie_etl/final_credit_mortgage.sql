create table if not exists dimensions_counselorgenie.f_credit_mortgage (
    credit_mortgage_id VARCHAR(64) PRIMARY KEY,
    credit_report_id varchar(64),
    pastDue int,
    dateClosed varchar(64),
    remarkCode varchar(64),
    dateOpened varchar(64),
    late90DaysTotal int,
    portfolioType varchar(64),
    scheduledMonthlyPayment int,
    ecoaDesignator varchar(64),
    late60DaysTotal int,
    creditLimit int,
    lenderIndustryCode varchar(64),
    accountRating varchar(64),
    accountType varchar(64),
    currentBalance int,
    highCredit int,
    accountNumber varchar(64),
    late30DaysTotal int,
    monthsReviewedCount int,
    lenderName varchar(128),
    lenderMemberCode varchar(64),
    environmentName varchar(64),
    createdAt timestamp,
    updatedAt timestamp,
    record_created_at timestamp
);

TRUNCATE TABLE dimensions_counselorgenie.f_credit_mortgage;

SET
    enable_case_sensitive_identifier to TRUE;

INSERT INTO
    dimensions_counselorgenie.f_credit_mortgage
SELECT
    rdata."id" :: varchar(64) as credit_installment_id,
    rdata."creditReportId" :: varchar(64) as credit_report_id,
    rdata."pastDue" :: int as pastDue,
    rdata."dateClosed" :: varchar(64) as dateClosed,
    rdata."remarkCode" :: varchar(64) as remarkCode,
    rdata."dateOpened" :: varchar(64) as dateOpened,
    rdata."late90DaysTotal" :: int as late90DaysTotal,
    rdata."portfolioType" :: varchar(64) as portfolioType,
    rdata."scheduledMonthlyPayment" :: int as scheduledMonthlyPayment,
    rdata."ecoaDesignator" :: varchar(64) as ecoaDesignator,
    rdata."late60DaysTotal" :: int as late60DaysTotal,
    rdata."creditLimit" :: int as creditLimit,
    rdata."lenderIndustryCode" :: varchar(64) as lenderIndustryCode,
    rdata."accountRating" :: varchar(64) as accountRating,
    rdata."accountType" :: varchar(64) as accountType,
    rdata."currentBalance" :: int as currentBalance,
    rdata."highCredit" :: int as highCredit,
    rdata."accountNumber" :: varchar(64) as accountNumber,
    rdata."late30DaysTotal" :: int as late30DaysTotal,
    rdata."monthsReviewedCount" :: int as monthsReviewedCount,
    rdata."lenderName" :: varchar(128) as lenderName,
    rdata."lenderMemberCode" :: varchar(64) as lenderMemberCode,
    rdata."environmentName" :: varchar(64) as environmentName,
    rdata."createdAt" :: timestamp as createdAt,
    rdata."updatedAt" :: timestamp as updatedAt,
    getdate() as record_created_at
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY rdata."id" :: varchar(64)
                ORDER BY
                    rdata."createdAt" :: timestamp DESC
            ) AS rn
        FROM
            staging_counselorgenie.staging_credit
    )
WHERE
    rn = 1
    AND rdata."environmentName" :: varchar(64) not in ('master', 'main', 'staging', 'prod')
    AND portfolioType :: varchar(64) = 'mortgage';