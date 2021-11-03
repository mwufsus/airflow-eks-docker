create table if not exists dimensions_counselorgenie.f_plaidaccounts (
    plaid_account_id varchar(64) PRIMARY KEY,
    plaid_item_id varchar(64),
    name varchar(128),
    subtype varchar(64),
    officialName varchar(128),
    type varchar(64),
    mask varchar(64),
    environmentName varchar(64),
    createdat timestamp,
    updatedAt timestamp,
    record_created_at timestamp
);

TRUNCATE TABLE dimensions_counselorgenie.f_plaidaccounts;

SET
    enable_case_sensitive_identifier to TRUE;

INSERT INTO
    dimensions_counselorgenie.f_plaidaccounts
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
            staging_counselorgenie.staging_plaidaccounts
    )
WHERE
    rn = 1
    AND rdata."environmentName" :: varchar(64) not in ('master', 'main', 'staging', 'prod');