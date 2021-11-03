COPY data_staging.creditgenie_app_decision
FROM 's3://datagenie-data/creditgenie-app/{{params.environment}}/Decision/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';

