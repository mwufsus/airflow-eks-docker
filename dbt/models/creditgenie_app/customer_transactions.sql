WITH source_data AS (
SELECT rdata."id"::varchar(64) as plaid_transaction_id,
    rdata."plaidAccountId"::varchar(64) as plaid_account_id,
    rdata."owner"::varchar(64) as owner,
    rdata."date"::timestamp as transaction_date,
    rdata."amount"::float as amount,
    rdata."pending"::varchar(128) as pending,
    rdata."type"::varchar(128) as type,
    rdata."paymentChannel"::varchar(128) as paymentchanel,
    rdata."name"::varchar(512) as name,
    rdata."authorizedDate"::varchar(128) as authorized_date,
    rdata."paymentMeta"."payee"::varchar(128) as payment_payee,
    rdata."paymentMeta"."ppId"::varchar(128) as ppid,
    rdata."paymentMeta"."referenceNumber"::varchar(128) as reference_number,
    rdata."paymentChannel"::varchar(128) as payment_channel,
    rdata."location"."country"::varchar(512) as country,
    rdata."location"."storeNumber"::varchar(512) as store_number,
    rdata."location"."address"::varchar(512) as address,
    rdata."location"."city"::varchar(512) as city,
    rdata."location"."postalCode"::varchar(512) as postal_code,
    rdata."location"."lon"::varchar(512) as longitude,
    rdata."location"."region"::varchar(64) as region,
    rdata."location"."lat"::varchar(512) as latitude,
    rdata."pendingTransactionId"::varchar(128) as pending_transaction_id,
    rdata."isoCurrencyCode"::varchar(128) as currency_code,
    rdata."categoryId"::varchar(128) as category_id,
    rdata."category"[0]::varchar(512) as category,
    rdata."environmentName"::varchar(128) as environment_name,
    rdata."createdAt"::timestamp as createdat,
    rdata."updatedAt"::timestamp as updatedat,
    getdate() as record_created_at
FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY rdata."id"::varchar(64)
                ORDER BY rdata."createdAt" DESC
            ) AS rn
        FROM 
            {{ source('data_staging', 'creditgenie_app_plaidtransaction')}}

    )
WHERE rn = 1
        AND rdata."environmentName"::varchar(24) in ('prod')
)
select *
from source_data