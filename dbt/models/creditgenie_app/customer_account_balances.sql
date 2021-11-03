WITH source_data AS( 
SELECT
    rdata."id" :: varchar(64) as plaidaccountbalance_id,
    rdata."plaidaccount_id" :: varchar(64) as plaidaccount_id,
    rdata."current_balance" :: varchar(64) as current_balance,
    rdata."available_balance" :: varchar(64) as available_balance,
    rdata."limit" :: varchar(64) as limit,
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
            {{ source('data_staging', 'creditgenie_app_plaidaccountbalance')}}
   
    )
WHERE
    rn = 1
        AND rdata."environmentName"::varchar(24) in ('prod')
)
select *
from source_data


