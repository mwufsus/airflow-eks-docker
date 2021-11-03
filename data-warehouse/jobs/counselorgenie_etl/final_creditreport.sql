create table if not exists dimensions_counselorgenie.f_counselorgenie_creditreport (
    creditreport_id varchar(64) PRIMARY KEY,
    borrower_id varchar(64),
    spouse_id varchar(64),
    s3key varchar(128),
    vantagescore int4,
    environmentname varchar(24),
    updatedat timestamp,
    createdat timestamp,
    record_created_at timestamp
);

TRUNCATE TABLE dimensions_counselorgenie.f_counselorgenie_creditreport;

SET
    enable_case_sensitive_identifier to TRUE;

INSERT INTO
    dimensions_counselorgenie.f_counselorgenie_creditreport
SELECT
    rdata."id" :: varchar(64) as creditreport_id,
    rdata."borrowerId" :: varchar(64) as borrower_id,
    rdata."spouseId" :: varchar(64) as spouse_id,
    rdata."s3key" :: varchar(128) as s3key,
    rdata."vantageScore" :: int4 as vantageScore,
    rdata."environmentName" :: varchar(24) as environmentName,
    rdata."updatedAt" :: timestamp as updatedAt,
    rdata."createdAt" :: timestamp as createdAt,
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
            staging_counselorgenie.staging_creditreport
    )
WHERE
    rn = 1
    AND rdata."environmentName" :: varchar(24) not in ('master', 'main', 'staging', 'prod');

UPDATE
    dimensions_counselorgenie.f_id_mappings
SET
    creditreport_id = creditreport.creditreport_id
FROM
    dimensions_counselorgenie.f_id_mappings mappings
    INNER JOIN dimensions_counselorgenie.f_counselorgenie_creditreport creditreport ON mappings.borrower_id = creditreport.borrower_id;