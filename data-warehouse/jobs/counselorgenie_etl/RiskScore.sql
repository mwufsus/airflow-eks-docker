COPY staging_counselorgenie.staging_riskscore
FROM 's3://datagenie-data/counselorgenie/RiskScore/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';
