-- With statement
with

    -- Import CTEs
    base_pokemon as (select * from {{ ref("stg_pokemon__pokemon") }}),

    -- Logical CTEs
    clean_pokemon_ability as (

        select

            raw_json:id::int as id,
            initcap(ability.value:ability:name::string) as ability_name,
            split(ability.value:ability:url::string, '/')[6]::int as ability_id,
            ability.value:is_hidden::string as is_hidden

        from base_pokemon, lateral flatten(input => raw_json:abilities) as ability

    )

-- Simple Select Statement
select *
from clean_pokemon_ability
