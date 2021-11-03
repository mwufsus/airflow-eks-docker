create table if not exists dimensions_counselorgenie.f_creditgenie_plaiditems (
    plaiditem_id varchar(64) PRIMARY KEY,
    borrower_id varchar(64),
    institution_id varchar(64),
    accesstoken varchar(64),
    isfinishedhistoricalupdate varchar(64),
    isfinishedinitialupdate varchar(64),
    environmentname varchar(64),
    createdat timestamp,
    updatedat timestamp,
    record_created_at timestamp
);

TRUNCATE TABLE dimensions_counselorgenie.f_creditgenie_plaiditems;

INSERT INTO
    dimensions_counselorgenie.f_creditgenie_plaiditems
SELECT
    rdata."id" :: varchar(64) as plaiditem_id,
    rdata."borrowerId" :: varchar(64) as borrower_id,
    rdata."institutionId" :: varchar(64) as institution_id,
    rdata."accessToken" :: varchar(64) as accessToken,
    rdata."isFinishedHistoricalUpdate" :: varchar(64) as isFinishedHistoricalUpdate,
    rdata."isFinishedInitialUpdate" :: varchar(64) as isFinishedInitialUpdate,
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
                    rdata."createdat" :: timestamp DESC
            ) AS rn
        FROM
            staging_counselorgenie.staging_plaiditems
    )
WHERE
    rn = 1
    AND rdata."environmentName" :: varchar(64) not in ('master', 'main', 'staging', 'prod');