CREATE TABLE IF NOT EXISTS dimensions_counselorgenie.f_counselorgenie_application (
    application_id varchar(64) PRIMARY KEY,
    hasrecentdeathinimmediatefamily bool,
    iscarowner bool,
    selfreportedmonthlycurrentincome int4,
    ishomeowner bool,
    hasreductioninincome bool,
    hasspouse bool,
    monthlyrentormortgageamount int4,
    totalmedicaldebtamount int4,
    numberofdependentsinhousehold int4,
    lastcompletedpage varchar(32),
    lastviewedpage varchar(32),
    hasrecentlybeenfurloughed bool,
    hasrecentjobloss bool,
    haslargemedicalordisabilityexpenses bool,
    verifiedmonthlycurrentincome int4,
    reasonfornotconnectingbank varchar(512),
    totalbacktaxesamount varchar(25),
    totaldebttofriendsandfamilyamount varchar(25),
    totalotherdebtamount varchar(25),
    hasservedinmilitary bool,
    hasmortgageescrow bool,
    kindofrelief varchar(128),
    environmentname varchar(24),
    router varchar(64),
    createdat timestamp,
    updatedat timestamp,
    record_created_at timestamp
);

TRUNCATE TABLE dimensions_counselorgenie.f_counselorgenie_application;

SET
    enable_case_sensitive_identifier to TRUE;

INSERT INTO
    dimensions_counselorgenie.f_counselorgenie_application
SELECT
    rdata."id" :: varchar(64) as id,
    rdata."hasRecentDeathInImmediateFamily" :: bool as hasRecentDeathInImmediateFamily,
    rdata."isCarOwner" :: bool as isCarOwner,
    rdata."selfReportedMonthlyCurrentIncome" :: int4 as selfReportedMonthlyCurrentIncome,
    rdata."isHomeOwner" :: bool as isHomeOwner,
    rdata."hasReductionInIncome" :: bool as hasReductionInIncome,
    rdata."hasSpouse" :: bool as hasSpouse,
    rdata."monthlyRentOrMortgageAmount" :: int4 as monthlyRentOrMortgageAmount,
    rdata."totalMedicalDebtAmount" :: int4 as totalMedicalDebtAmount,
    rdata."numberOfDependentsInHousehold" :: int4 as numberOfDependentsInHousehold,
    rdata."lastCompletedPage" :: varchar(32) as lastCompletedPage,
    rdata."lastViewedPage" :: varchar(32) as lastViewedPage,
    rdata."hasRecentlyBeenFurloughed" :: bool as hasRecentlyBeenFurloughed,
    rdata."hasRecentJobLoss" :: bool as hasRecentJobLoss,
    rdata."hasLargeMedicalOrDisabilityExpenses" :: bool as hasLargeMedicalOrDisabilityExpenses,
    rdata."verifiedMonthlyCurrentIncome" :: int4 as verifiedMonthlyCurrentIncome,
    rdata."reasonForNotConnectingBank" :: varchar(512) as reasonForNotConnectingBank,
    rdata."totalBackTaxesAmount" :: varchar(25) as totalBackTaxesAmount,
    rdata."totalDebtToFriendsAndFamilyAmount" :: varchar(25) as totalDebtToFriendsAndFamilyAmount,
    rdata."totalOtherDebtAmount" :: varchar(25) as totalOtherDebtAmount,
    rdata."hasServedInMilitary" :: bool as hasServedInMilitary,
    rdata."hasMortgageEscrow" :: bool as hasMortgageEscrow,
    rdata."kindOfRelief" :: varchar(128) as kindOfRelief,
    rdata."environmentName" :: varchar(24) as environmentName,
    rdata."router" :: varchar(64) as router,
    rdata."createdAt" :: timestamp as createdAt,
    rdata."updatedAt" :: timestamp as updatedAt,
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
            staging_counselorgenie.staging_application
    )
WHERE
    rn = 1
    AND rdata."environmentName" :: varchar(24) not in ('master', 'main', 'prod', 'staging');