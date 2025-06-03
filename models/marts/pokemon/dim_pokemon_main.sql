-- With statement
with

    -- Import CTEs
    base_pokemon as (select * from {{ ref("stg_pokemon__pokemon") }}),

    -- Logical CTEs
    clean_pokemon as (

        select

            initcap(raw_json:name::string) as name,
            raw_json:id::int as id,
            split(raw_json:species.url::string, '/')[6]::int as species_id,
            raw_json:height::int / 10 as height,
            raw_json:weight::int / 10 as weight,
            initcap(raw_json:types[0].type.name::string) as type_1,
            initcap(raw_json:types[1].type.name::string) as type_2,
            ifnull(type_1 || '-' || type_2, type_1) as type_name,
            raw_json:stats[0].base_stat::int as hp,
            raw_json:stats[1].base_stat::int as attack,
            raw_json:stats[2].base_stat::int as defence,
            raw_json:stats[3].base_stat::int as special_attack,
            raw_json:stats[4].base_stat::int as special_defence,
            raw_json:stats[5].base_stat::int as speed,
            hp
            + attack
            + defence
            + special_attack
            + special_defence
            + speed as base_stat_total

        from base_pokemon

    )

-- Simple Select Statement
select *
from clean_pokemon
