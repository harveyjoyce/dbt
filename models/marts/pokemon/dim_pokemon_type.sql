-- With statement
with

    -- Import CTEs
    pokemon_main as (select * from {{ ref("dim_pokemon_main") }}),

    -- Logical CTEs
    clean_pokemon_type as (

        select

            id as id,
            type_value as type_value,
            right(type_number, 1)::int as type_number

        from pokemon_main unpivot (type_value for type_number in (type_1, type_2))

        order by id, type_number

    )

-- Simple Select Statement
select *
from clean_pokemon_type
