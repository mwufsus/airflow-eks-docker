{% macro rowNumOverPartByOrderBy(sourceName, tableName, partitionedBy, orderedBy) %}
        (
            select
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY {{ partitionedBy }}
                    ORDER BY
                        {{ orderedBy }} DESC
                ) AS rn
            from
                {{ source( sourceName ,  tableName ) }}
        )
{% endmacro %}