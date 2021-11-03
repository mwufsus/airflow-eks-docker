WITH source_data as (
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
            {{ source('data_staging', 'creditgenie_app_plaiditem')}}

    )
WHERE
    rn = 1
        AND rdata."environmentName"::varchar(24) in ('prod')
)
select *
from source_data