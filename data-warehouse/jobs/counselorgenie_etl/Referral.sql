COPY staging_counselorgenie.staging_referral
FROM 's3://datagenie-data/referralgenie/Referral/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
FORMAT JSON 'noshred';
