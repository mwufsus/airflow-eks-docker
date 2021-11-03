WITH source_data AS (
SELECT
    rdata."id" :: varchar(64) as plaid_account_id,
    rdata."plaidItemId" :: varchar(64) as plaid_item_id,
    rdata."name" :: varchar(128) as name,
    rdata."subType" :: varchar(64) as subType,
    rdata."officialName" :: varchar(128) as officialName,
    rdata."type" :: varchar(64) as type,
    rdata."mask" :: varchar(64) as mask,
    rdata."environmentName" :: varchar(64) as environmentName,
    rdata."createdAt" :: timestamp as createdAt,
    rdata."updatedAt" :: timestamp as updatedAt,
    getdate() as record_created_at
FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY rdata."id" :: varchar(64)
                ORDER BY
                    rdata."createdat" :: timestamp DESC
            ) AS rn
        FROM
            {{ source('data_staging', 'creditgenie_app_plaidaccount')}}

    )
WHERE
    rn = 1
        AND rdata."environmentName"::varchar(24) in ('prod')
)
SELECT *
FROM source_data