CREATE TABLE IF NOT EXISTS dimensions_counselorgenie.f_creditgenie_plaidtransactions (
    plaid_transaction_id varchar(64) PRIMARY KEY,
    plaid_account_id varchar(64),
    owner varchar(64),
    transaction_date timestamp,
    amount double precision,
    pending varchar(128),
    type varchar(128),
    paymentchannel varchar(128),
    name varchar(512),
    authorized_date varchar(128),
    payment_payee varchar(128),
    ppid varchar(128),
    reference_number varchar(128),
    payment_channel varchar(128),
    location_country varchar(512),
    location_store_number varchar(512),
    location_address varchar(512),
    location_city varchar(512),
    location_postalCode varchar(512),
    location_lon varchar(512),
    location_region varchar(64),
    location_lat varchar(512),
    pending_transaction_id varchar(128),
    currency_code varchar(128),
    category_id varchar(128),
    category varchar(512),
    environment_name varchar(128),
    createdat timestamp,
    updatedAt timestamp,
    record_created_at timestamp
);

TRUNCATE TABLE dimensions_counselorgenie.f_creditgenie_plaidtransactions;

INSERT INTO dimensions_counselorgenie.f_creditgenie_plaidtransactions
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
        FROM staging_counselorgenie.staging_plaidtransaction
    )
WHERE rn = 1
    AND rdata."environmentName"::varchar(128) not in ('master', 'main', 'staging', 'prod');