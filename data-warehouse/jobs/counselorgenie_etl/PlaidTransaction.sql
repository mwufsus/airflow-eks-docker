COPY staging_counselorgenie.staging_plaidtransaction 
FROM 's3://datagenie-data/counselorgenie/PlaidTransaction/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';
