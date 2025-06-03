-- With statement
with

    -- Import CTEs
    base_pokemon as (select * from {{ ref("stg_pokemon__pokemon") }}),

    -- Logical CTEs
    clean_pokemon_stats as (

        select

            raw_json:id::int as id,
            initcap(stat.value:stat:name::string) as stat_name,
            stat.value:base_stat::int as stat_value,

        from base_pokemon, lateral flatten(input => raw_json:stats) as stat

    )

-- Simple Select Statement
select *
from clean_pokemon_stats
