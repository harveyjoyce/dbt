-- Input CTE

With 

pokemon_main as (

    select * from {{ ref ('dim_pokemon_main')}}
),

pokemon_sprite as (

    select * from {{ ref ('dim_pokemon_sprite')}}
),

pokemon_type as (

    select * from {{ ref ('dim_pokemon_type')}}
),

pokemon_stats as (

    select * from {{ ref ('dim_pokemon_stats')}}
),

species_main as (

    select * from {{ ref ('dim_species_main')}}
),

species_egg_group as (

    select * from {{ ref ('dim_species_egg_group')}}
),

evolution_chain_main as (

    select * from {{ ref ('dim_evolution_chain_main')}}
),

pokemon_ability as (

    select * from {{ ref ('dim_pokemon_ability')}}
),

ability_main as (

    select * from {{ ref ('dim_ability_main')}}
),

pokemon_move as (

    select * from {{ ref ('dim_pokemon_move')}}
),

move_main as (

    select * from {{ ref ('dim_move_main')}}
),

type_defensive as (

    select * from {{ ref ('dim_type_defensive')}}
)

-- Final CTE

select *

from pokemon_main
inner join pokemon_sprite
    on Pmain_ID = Psprite_ID
inner join pokemon_type
    on Pmain_ID = Ptype_ID
inner join pokemon_stats
    on Pmain_ID = Pstat_ID

-- Species Info
inner join species_main
    on Pmain_SPECIES_ID = Smain_SPECIES_ID
inner join species_egg_group
    on Smain_SPECIES_ID = Segg_SPECIES_ID
inner join evolution_chain_main
    on Smain_CHAIN_ID = ECmain_chain_id

-- Ability Info
inner join pokemon_ability
    on Pmain_ID = Pabil_ID
inner join ability_main
    on Pabil_ABILITY_ID = Amain_ABILITY_ID

-- Move Info
inner join pokemon_move
    on Pmain_ID = Pmove_ID
inner join move_main
    on Pmove_MOVE_ID = Mmain_MOVE_ID

inner join type_defensive
    on Pmain_Type_Name = Tdef_type_name
where Pmain_ID = 1