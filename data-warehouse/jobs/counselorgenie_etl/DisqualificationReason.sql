COPY staging_counselorgenie.staging_disqualifications 
FROM 's3://datagenie-data/counselorgenie/DisqualificationReason/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';