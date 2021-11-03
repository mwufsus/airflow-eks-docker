CREATE TABLE IF NOT EXISTS dimensions_counselorgenie.f_utm_applications (
    application_id varchar(64) PRIMARY KEY,
    utmcampaign VARCHAR(128),
    utmcontent VARCHAR(128),
    utmmedium VARCHAR(128),
    utmsource VARCHAR(128),
    utmterm VARCHAR(128),
    environmentname varchar(24),
    createdat timestamp,
    updatedat timestamp,
    record_created_at timestamp
);

SET
    enable_case_sensitive_identifier to TRUE;

TRUNCATE TABLE dimensions_counselorgenie.f_utm_applications;

INSERT INTO
    dimensions_counselorgenie.f_utm_applications
SELECT
    rdata."id" :: varchar(64) as application_id,
    rdata."utmCampaign" :: varchar(128) as utmcampaign,
    rdata."utmcontent" :: varchar(128) as utmcontent,
    rdata."utMmedium" :: varchar(128) as utmmedium,
    rdata."utmSource" :: varchar(128) as utmsource,
    rdata."utmTerm" :: varchar(128) as utmterm,
    rdata."environmentName" :: varchar(128) as environmentName,
    rdata."createdAt" :: timestamp as createdat,
    rdata."updatedAt" :: timestamp as updatedAt,
    getdate() as record_created_at
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY rdata."id" :: varchar(64)
                ORDER BY
                    rdata."createdAt" :: varchar(128) DESC
            ) AS rn
        from
            staging_counselorgenie.staging_application
    )
WHERE
    rn = 1
    AND rdata."environmentName" :: varchar(128) not in ('master', 'main', 'staging', 'prod');