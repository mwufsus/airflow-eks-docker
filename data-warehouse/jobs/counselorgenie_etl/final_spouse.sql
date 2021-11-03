create table if not exists dimensions_counselorgenie.f_creditgenie_spouse (
    spouse_id varchar(64) PRIMARY KEY,
    spouse_borrower_id varchar(64),
    firstname varchar(64),
    lastname varchar(64),
    addressline1 varchar(128),
    addressline2 varchar(64),
    city varchar(64),
    state varchar(55),
    zipcode varchar(15),
    dateofbirth varchar(10),
    phonenumber varchar(12),
    email varchar(128),
    environmentname varchar(24),
    createdat timestamp,
    updatedat timestamp,
    record_created_at timestamp
);

SET
    enable_case_sensitive_identifier to TRUE;

TRUNCATE TABLE dimensions_counselorgenie.f_creditgenie_spouse;

INSERT INTO
    dimensions_counselorgenie.f_creditgenie_spouse
SELECT
    rdata."id" :: varchar(64) as spouse_id,
    rdata."spouseBorrowerId" :: varchar(64) as spouse_borrower_id,
    rdata."firstName" :: varchar(64),
    rdata."lastName" :: varchar(64),
    rdata."addressLine1" :: varchar(128),
    rdata."addressLine2" :: varchar(64),
    rdata."city" :: varchar(64),
    rdata."state" :: varchar(55),
    rdata."zipCode" :: varchar(15),
    rdata."dateOfBirth" :: varchar(10),
    rdata."phoneNumber" :: varchar(12),
    rdata."email" :: varchar(128),
    rdata."environmeNtname" :: varchar(24),
    rdata."createdAt" :: timestamp,
    rdata."updatedAt" :: timestamp,
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
            staging_counselorgenie.staging_spouse
    )
WHERE
    rn = 1
    AND rdata."environmentName" :: varchar(24) not in ('master', 'main', 'staging', 'prod');