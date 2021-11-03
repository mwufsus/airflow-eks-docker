WITH source_data AS (
SELECT
    rdata."id" :: varchar(64) as disqualification_id,
    rdata."decisionId" :: varchar(64) as decision_id,
    rdata."CustomerId" :: varchar(64) as customer_id,
    rdata."reason" :: varchar(64) as reason,
    rdata."environmentname" :: varchar(128) as environmentname,
    rdata."createdAt" :: timestamp as createdat,
    rdata."updatedAt" :: timestamp as updatedat,
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
            {{ source('data_staging', 'creditgenie_app_disqualificationreason')}}

    )
WHERE
    rn = 1
        AND rdata."environmentName"::varchar(24) in ('prod')
 )
SELECT *
FROM source_data   