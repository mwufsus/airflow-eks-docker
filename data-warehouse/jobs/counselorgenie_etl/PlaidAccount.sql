COPY staging_counselorgenie.staging_plaidaccounts
FROM 's3://datagenie-data/counselorgenie/PlaidAccount/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';