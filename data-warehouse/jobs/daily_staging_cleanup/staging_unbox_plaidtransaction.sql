update
    staging.staging_plaidtransaction
set
    high_category = unbox.high_category,
    granular_category = unbox.granular_category,
    type_category = unbox.type_category
from
    (
        select
            id,
            regexp_replace(
                split_part(category, ',', 1),
                '[^a-zA-Z0-9]+',
                ''
            ) as high_category,
            regexp_replace(
                split_part(category, ',', 2),
                '[^a-zA-Z0-9]+',
                ''
            ) as granular_category,
            regexp_replace(
                split_part(category, ',', 3),
                '[^a-zA-Z0-9]+',
                ''
            ) as type_category
        from
            staging.staging_plaidtransaction
        where
            environmentname not in ('master', 'staging', 'prod', 'main')
    ) unbox
where
    staging_plaidtransaction.id = unbox.id
    and staging_plaidtransaction.high_category is null
    and staging_plaidtransaction.category is not null;