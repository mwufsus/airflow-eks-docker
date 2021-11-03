create table if not exists dimensions_counselorgenie.f_counselorgenie_decisions (
    decision_id varchar(64),
    application_id varchar(64),
    owner varchar(64),
    current_gross_income float,
    proposed_net_cashflow float,
    current_net_cashflow float,
    current_expenses float,
    proposed_expenses float,
    number_of_unsecured_debts float,
    total_unsecured_debt_balance float,
    current_unsecured_debt_payment float,
    proposed_unsecured_debt_payment float,
    current_disposable_income float,
    proposed_disposable_income float,
    is_qualified bool,
    environment_name varchar(24),
    created_at timestamp,
    updated_at timestamp,
    record_created_at timestamp
);
TRUNCATE TABLE dimensions_counselorgenie.f_counselorgenie_decisions;
SET enable_case_sensitive_identifier to TRUE;
INSERT INTO dimensions_counselorgenie.f_counselorgenie_decisions
SELECT rdata."id"::varchar(64) as decision_id,
    rdata."applicationId"::varchar(64) as application_id,
    rdata."owner"::varchar(64) as owner,
    rdata."currentGrossIncome"::float as current_gross_income,
    rdata."proposedNetCashflow"::float as proposed_net_cashflow,
    rdata."currentNetCashflow"::float as current_net_cashflow,
    rdata."currentExpenses"::float as current_expenses,
    rdata."proposedExpenses"::float as proposed_expenses,
    rdata."numberOfUnsecuredDebts"::float as number_of_unsecured_debts,
    rdata."totalUnsecuredDebtBalance"::float as total_unsecured_debt_balance,
    rdata."currentUnsecuredDebtPayment"::float as current_unsecured_debt_payment,
    rdata."proposedUnsecuredDebtPayment"::float as proposed_unsecured_debt_payment,
    rdata."currentDisposableIncome"::float as current_disposable_income,
    rdata."proposedDisposableIncome"::float as proposed_disposable_income,
    rdata."isQualified"::bool as is_qualified,
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
        FROM staging_counselorgenie.staging_decision
    )
WHERE rn = 1
    AND rdata."environmentName"::varchar(24) not in ('master', 'main', 'staging', 'prod')