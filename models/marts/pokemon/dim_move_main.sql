-- With statement
with

    -- Import CTEs
    base_pokemon_move as (select * from {{ ref("stg_pokemon__move") }}),

    -- Logical CTEs
    flavour_text as (

        select
        
            raw_json:id::int as move_id,
            ft.value:flavor_text::string as flavour_text,
            split(ft.value:version_group:url::string, '/')[6]::int as gameid

        from
            base_pokemon_move,
            lateral flatten(input => raw_json:flavor_text_entries) as ft

        where
            ft.value:language:name::string = 'en'
            and ft.value:flavor_text != 'This move can’t be used.
        It’s recommended that this move is forgotten.
        Once forgotten, this move can’t be remembered.'

        group by move_id, flavour_text, gameid

        qualify row_number() over (partition by move_id order by gameid desc) = 1

    ),

    clean_move as (

        select

            initcap(raw_json:name::string) as move_name,
            raw_json:id::int as move_id,
            raw_json:generation:name::string as debut_gen,
            initcap(raw_json:type:name::string) as type,
            initcap(raw_json:damage_class:name::string) as damage_class,
            raw_json:power::int as power,
            raw_json:accuracy::int as accuracy,
            raw_json:pp::int as pp,
            raw_json:priority::int as priority,
            flavour_text as flavour_text,
            raw_json:effect_entries[0]:short_effect::string as short_effect

        from base_pokemon_move mt

        left join flavour_text ft on raw_json:id::int = ft.move_id

        order by move_id

    )

-- Simple Select Statement
select *
from clean_move
