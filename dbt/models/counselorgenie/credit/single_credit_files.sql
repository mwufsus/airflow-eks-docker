WITH source_data as (
    SELECT
        *
    FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY accountnumber
                    ORDER BY
                        createdat DESC
                ) AS rn
            FROM
                staging.staging_credit
                WHERE currentbalance > 0 
        )
    WHERE
        rn = 1
        AND environmentname NOT IN ('master', 'main', 'staging', 'prod')
)
SELECT *
FROM counselorgenie_staging.staging_credit