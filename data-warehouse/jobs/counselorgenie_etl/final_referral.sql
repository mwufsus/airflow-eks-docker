create table if not exists dimensions_counselorgenie.f_counselorgenie_referral (
    referral_id VARCHAR(64) PRIMARY KEY,
    owner VARCHAR(64),
    email VARCHAR(128),
    referral_first_name VARCHAR(64),
    environmentname VARCHAR(64),
    createdat timestamp,
    updatedat timestamp,
    record_created_at timestamp
);

TRUNCATE TABLE dimensions_counselorgenie.f_counselorgenie_referral;

SET
    enable_case_sensitive_identifier to TRUE;

INSERT INTO
    dimensions_counselorgenie.f_counselorgenie_referral
SELECT
    rdata."id" :: varchar(64) as referral_id,
    rdata."owner" :: varchar(64) as owner,
    rdata."email" :: varchar(64) as email,
    rdata."firstName" :: varchar(64) as referral_first_name,
    rdata."environmentName" :: varchar(64) as environmentname,
    rdata."createdAt" :: timestamp as createdat,
    rdata."updatedAt" :: timestamp as updatedat,
    getdate() as record_created_at
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY rdata."id" :: varchar(64)
                ORDER BY
                    rdata."createdAt" DESC
            ) AS rn
        FROM
            staging_counselorgenie.staging_referral
    )
WHERE
    rn = 1
    AND rdata."environmentName" :: varchar(64) not in ('master', 'main', 'staging', 'prod');