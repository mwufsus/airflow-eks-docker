COPY staging_counselorgenie.staging_debtscore
FROM 's3://datagenie-data/counselorgenie/DebtScore/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';