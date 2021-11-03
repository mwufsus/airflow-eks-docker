COPY staging_counselorgenie.staging_collections
FROM 's3://datagenie-data/counselorgenie/Collection/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';