COPY staging_counselorgenie.staging_expenses 
FROM 's3://datagenie-data/counselorgenie/Expense/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';
