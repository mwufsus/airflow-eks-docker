WITH source_data AS (
    SELECT rdata."id"::varchar(64) as customer_id,
        rdata."firstName"::varchar(128) as first_name,
        rdata."lastName"::varchar(128) as last_name,
        rdata."phoneNumber"::varchar(25) as phone_number,
        rdata."email"::varchar(128) as email,
        rdata."environmentName"::varchar(128) as environment_name,
        rdata."createdAt"::timestamp as created_at,
        rdata."updatedAt"::timestamp as updated_at,
        getdate() as record_created_at
    FROM (
            select *,
                ROW_NUMBER() OVER (
                    PARTITION BY rdata."id"::varchar(64)
                    ORDER BY rdata."createdAt"::timestamp DESC
                ) AS rn
            FROM 
            {{ source('data_staging', 'creditgenie_app_customer')}}

        )
    WHERE rn = 1
        AND rdata."environmentName"::varchar(24) in ('prod')
)
SELECT *
FROM source_data