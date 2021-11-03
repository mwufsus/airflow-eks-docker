COPY staging_counselorgenie.staging_credit
FROM 's3://datagenie-data/counselorgenie/Debt/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';