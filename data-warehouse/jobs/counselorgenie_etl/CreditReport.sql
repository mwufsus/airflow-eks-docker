COPY staging_counselorgenie.staging_creditreport
FROM 's3://datagenie-data/counselorgenie/CreditReport/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';