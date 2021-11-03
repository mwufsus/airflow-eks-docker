create table if not exists dimensions_counselorgenie.f_counselorgenie_disqualifications (
    disqualification_id varchar(64),
    decision_id varchar(64),
    owner varchar(64),
    reason varchar(128),
    environment_name varchar(24),
    created_at timestamp,
    updated_at timestamp,
    record_created_at timestamp
);
TRUNCATE TABLE dimensions_counselorgenie.f_counselorgenie_disqualifications;
SET enable_case_sensitive_identifier to TRUE;
INSERT INTO dimensions_counselorgenie.f_counselorgenie_disqualifications
SELECT rdata."id"::varchar(64) as disqualification_id,
    rdata."decisionId"::varchar(64) as decision_id,
    rdata."owner"::varchar(64) as owner,
    rdata."reason"::varchar(128) as reason,
    rdata."environmentName"::varchar(24) as environment_name,
    rdata."createdAt"::timestamp as created_at,
    rdata."updatedAt"::timestamp as updated_at,
    GETDATE() AS record_created_at
FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY rdata."id"::varchar(64)
                ORDER BY rdata."createdAt"::timestamp DESC
            ) AS rn
        FROM staging_counselorgenie.staging_disqualifications
    )
WHERE rn = 1
    AND rdata."environmentName"::varchar(24) not in ('master', 'main', 'staging', 'prod')