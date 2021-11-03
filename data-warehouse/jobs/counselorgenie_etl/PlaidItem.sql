COPY staging_counselorgenie.staging_plaiditems
FROM 's3://datagenie-data/counselorgenie/PlaidItem/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';
