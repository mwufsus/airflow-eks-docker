copy staging_counselorgenie.staging_spouse
FROM 's3://datagenie-data/counselorgenie/Spouse/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';
