-- With statement
with

    -- Import CTEs
    base_pokemon as (select * from {{ ref("stg_pokemon__pokemon") }}),

    -- Logical CTEs
    clean_pokemon_moves as (

        select

            raw_json:id::int as id,
            initcap(move.value:move:name::string) as move_name,
            split(move.value:move:url::string, '/')[6]::int as move_id,
            details.value:level_learned_at::int as level_learned_at,
            initcap(details.value:move_learn_method:name::string) as learn_method,
            initcap(details.value:version_group:name::string) as version_group,
            split(details.value:version_group:url, '/')[6]::int as version_group_id

        from
            base_pokemon,
            lateral flatten(input => raw_json:moves) as move,
            lateral flatten(input => move.value:version_group_details) as details
    )

-- Simple Select Statement
select *
from clean_pokemon_moves
