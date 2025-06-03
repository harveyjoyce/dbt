-- With statement
with

    -- Import CTEs
    base_pokemon_species as (select * from {{ ref("stg_pokemon__species") }}),

    -- Logical CTEs
    clean_pokemon_species as (

        select

            initcap(raw_json:name::string) as name,
            raw_json:id::int as species_id,
            species.value:genus::string as species_name,
            raw_json:generation.name::string as debut_gen,
            split(raw_json:evolution_chain.url::string, '/')[6]::int as chain_id,
            raw_json:is_baby::string as is_baby,
            raw_json:is_legendary::string as is_legendary,
            raw_json:is_mythical::string as is_mythical

        from base_pokemon_species, lateral flatten(input => raw_json:genera) species
        where species.value:language.name = 'en'

    )

-- Simple Select Statement
select *
from clean_pokemon_species
