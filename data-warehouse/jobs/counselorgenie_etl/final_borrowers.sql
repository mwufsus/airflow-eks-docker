CREATE TABLE IF NOT EXISTS dimensions_counselorgenie.f_counselorgenie_borrowers (
    borrower_id varchar(64) PRIMARY KEY,
    application_id varchar(64), 
    lastname varchar(64),
    zipcode varchar(64),
    preferredlanguage varchar(64),
    city varchar(64),
    dateofbirth varchar(64),
    firstname varchar(64),
    phonenumber varchar(64),
    addressline1 varchar(64),
    addressline2 varchar(64),
    state varchar(64),
    email varchar(128),
    environmentname varchar(64),
    createdat timestamp,
    updatedat timestamp,
    record_created_at timestamp
);

TRUNCATE TABLE dimensions_counselorgenie.f_counselorgenie_borrowers;

SET
    enable_case_sensitive_identifier to TRUE;

INSERT INTO
    dimensions_counselorgenie.f_counselorgenie_borrowers
SELECT
    rdata."id" :: varchar(64) as borrower_id,
    rdata."borrowerApplicationId" :: varchar(64) as application_id,
    rdata."lastName" :: varchar(64) as lastName,
    rdata."zipCode" :: varchar(64) as zipCode,
    rdata."preferredLanguage" :: varchar(64) as preferredLanguage,
    rdata."city" :: varchar(64) as city,
    rdata."dateOfBirth" :: varchar(64) as dateOfBirth,
    rdata."firstName" :: varchar(64) as firstName,
    rdata."phoneNumber" :: varchar(64) as phoneNumber,
    rdata."addressLine1" :: varchar(64) as addressLine1,
    rdata."addressLine2" :: varchar(64) as addressLine2,
    rdata."state" :: varchar(64) as "state",
    rdata."email" :: varchar(128) as email,
    rdata."environmentName" :: varchar(64) as environmentName,
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
            staging_counselorgenie.staging_borrower
    )
WHERE
    rn = 1
    AND rdata."environmentName" :: varchar(64) not in ('master', 'main', 'staging', 'prod');