update
    staging_creditgenie.staging_plaidtransaction_cg
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
            staging_creditgenie.staging_plaidtransaction_cg
        where
            environmentname in ('prod')
    ) unbox
where
    staging_plaidtransaction_cg.id = unbox.id
    and staging_plaidtransaction_cg.high_category is null
    and staging_plaidtransaction_cg.category is not null;