COPY staging.staging_incomesnapshot (
__typename,
id,
owner,
borrowerid,
annualincome,
incometransactionids,
ninetydayincome,
applicationid,
monthlyincome,
createdat,
updatedat,
environmentname)
FROM 's3://datagenie-data/prod/procurement/IncomeSnapshot/{{params.date_string}}/'
IAM_ROLE 'arn:aws:iam::396136447318:role/RedShiftProdRole'
timeformat 'YYYY-MM-DDTHH:MI:SS'
json as 's3://datagenie-data/json_paths/counselorgenie/incomesnapshot_path.json';