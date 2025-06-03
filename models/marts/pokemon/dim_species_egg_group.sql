-- With statement
with

    -- Import CTEs
    base_pokemon_species as (select * from {{ ref("stg_pokemon__species") }}),

    -- Logical CTEs
    clean_pokemon_egg_group as (

        select

            initcap(raw_json:name::string) as name,
            raw_json:id::int as species_id,
            initcap(egg.value:name::string) as egg_group_name,
            split(egg.value:url::string, '/')[6]::int as egg_group_id

        from base_pokemon_species, lateral flatten(input => raw_json:egg_groups) as egg

    )

-- Simple Select Statement
select *
from clean_pokemon_egg_group
