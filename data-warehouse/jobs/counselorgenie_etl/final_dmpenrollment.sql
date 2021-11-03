create table if not exists dimensions_counselorgenie.f_counselorgenie_dmpenrollment (
    dmp_enrollment_id VARCHAR(64) PRIMARY KEY,
    application_id VARCHAR(128),
    owner varchar(64),
    active VARCHAR(64),
    firstpaymentdate VARCHAR(64),
    monthlypaymentamount int,
    date_signed VARCHAR(128), 
    environmentname VARCHAR(128), 
    createdAt timestamp,
    updatedat timestamp,
    record_created_at timestamp
);

TRUNCATE TABLE dimensions_counselorgenie.f_counselorgenie_dmpenrollment;

SET
    enable_case_sensitive_identifier to TRUE;

INSERT INTO
    dimensions_counselorgenie.f_counselorgenie_dmpenrollment
SELECT
    rdata."id" :: varchar(64) as dmp_enrollment_id,
    rdata."dmpEnrollmentApplicationId" :: varchar(64) as application_id,
    rdata."active" :: varchar(64) as active,
    rdata."firstPaymentDate" :: varchar(64) as firstPaymentDate,
    rdata."monthlyPaymentAmount" :: int as monthlyPaymentAmount,
    rdata."date_Signed" :: varchar(128) as date_signed,
    rdata."environmentName" :: varchar(128) as environmentName,
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
            staging_counselorgenie.staging_dmpenrollment
    )
WHERE
    rn = 1
    AND rdata."environmentName" :: varchar(24) not in ('master', 'main', 'staging', 'prod');