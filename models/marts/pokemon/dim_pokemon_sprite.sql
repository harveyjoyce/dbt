-- With statement
with

    -- Import CTEs
    base_pokemon as (select * from {{ ref("stg_pokemon__pokemon") }}),

    -- Logical CTEs
    clean_pokemon_sprite as (

        select

            raw_json:id::int as id,
            initcap(raw_json:name::string) as name,
            split(raw_json:species.url::string, '/')[6]::int as species_id,
            ifnull(
                raw_json:sprites.front_default::string,
                concat(
                    'https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/',
                    species_id::string,
                    '.png'
                )
            ) as sprite_url

        from base_pokemon

    )

-- Simple Select Statement
select *
from clean_pokemon_sprite
