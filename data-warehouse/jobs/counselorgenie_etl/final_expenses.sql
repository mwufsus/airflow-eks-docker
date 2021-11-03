create table if not exists dimensions_counselorgenie.f_counselorgenie_expenses (
    expenses_id VARCHAR(64) PRIMARY KEY,
    application_id VARCHAR(64),
    expenseblscexid VARCHAR(64),
    category VARCHAR(128),
    subcategory VARCHAR(128),
    estimatedvalue FLOAT,
    selfreportedvalue FLOAT,
    environmentname VARCHAR(128),
    createdAt timestamp,
    updatedat timestamp,
    record_created_at timestamp
);

TRUNCATE TABLE dimensions_counselorgenie.f_counselorgenie_expenses;

SET
    enable_case_sensitive_identifier to TRUE;

INSERT INTO
    dimensions_counselorgenie.f_counselorgenie_expenses
SELECT
    rdata."id" :: varchar(64) as expenses_id,
    rdata."applicationId" :: varchar(64) as application_id,
    rdata."expenseBlsCexId" :: varchar(64) as expenseBlsCexId,
    rdata."category" :: varchar(128) as category,
    rdata."subcategory" :: varchar(128) as subcategory,
    rdata."estimatedValue" :: float as estimatedValue,
    rdata."selfReportedValue" :: float as selfReportedValue,
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
            staging_counselorgenie.staging_expenses
    )
WHERE
    rn = 1
    AND rdata."environmentName" :: varchar(128) not in ('master', 'main', 'staging', 'prod');