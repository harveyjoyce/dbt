-- With statement
with

    -- Import CTEs
    base_pokemon_ability as (select * from {{ ref("stg_pokemon__ability") }}),

    -- Logical CTEs
    clean_ability as (

        select

            initcap(raw_json:name::string) as ability_name,
            raw_json:id::int as ability_id,
            raw_json:generation:name::string as debut_gen,
            ft.value:flavor_text::string as flavour_text,
            split(ft.value:version_group:url::string, '/')[6]::int as gameid

        from
            base_pokemon_ability,
            lateral flatten(input => raw_json:flavor_text_entries) as ft

        where ft.value:language:name::string = 'en'

        group by ability_name, ability_id, debut_gen, flavour_text, gameid

        qualify row_number() over (partition by ability_id order by gameid desc) = 1

    )

-- Simple Select Statement
select *
from clean_ability
