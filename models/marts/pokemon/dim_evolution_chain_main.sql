-- With statement
with

    -- Import CTEs
    base_evolution_chain as (select * from {{ ref("stg_pokemon__evolution_chain") }}),

    -- Logical CTEs
    stage_3 as (

        select

            raw_json:id::int as chain_id,
            initcap(stage2.value:species:name::string) as s2,
            initcap(stage3.value:species:name::string) as s3,
            split(stage3.value:species.url::string, '/')[6]::int as s3_dex

        from
            base_evolution_chain,
            lateral flatten(input => raw_json:chain:evolves_to) as stage2,
            lateral flatten(input => stage2.value:evolves_to) as stage3

    ),

    stage_2 as (

        select

            raw_json:id::int as chain_id,
            initcap(stage2.value:species:name::string) as s2,
            split(stage2.value:species.url::string, '/')[6]::int as s2_dex

        from
            base_evolution_chain,
            lateral flatten(input => raw_json:chain:evolves_to) as stage2

    ),

    stage_1 as (

        select

            raw_json:id::int as chain_id,
            initcap(raw_json:chain:species:name::string) as s1,
            split(raw_json:chain:species.url::string, '/')[6]::int as s1_dex

        from base_evolution_chain

    ),

    join_tables as (

        select a.chain_id, a.s1, a.s1_dex, b.s2, b.s2_dex, c.s3, c.s3_dex

        from stage_1 a

        left join stage_2 b on a.chain_id = b.chain_id

        left join stage_3 c on b.chain_id = c.chain_id and b.s2 = c.s2

    ),

    table_to_pivot as (

        select

            *,
            row_number() over (
                partition by chain_id order by s2_dex, s3_dex
            ) as evo_chain_id

        from join_tables

        order by chain_id, s1_dex, s2_dex, s3_dex

    ),

    pivot_name as (

        select chain_id, evo_chain_id, stage, stage_number

        from table_to_pivot unpivot (stage_number for stage in (s1, s2, s3))

    ),

    pivot_dex as (

        select chain_id, evo_chain_id, replace(dex, '_DEX', '') as dex, dex_number

        from table_to_pivot unpivot (dex_number for dex in (s1_dex, s2_dex, s3_dex))

    ),

    clean_evolution_chain as (

        select

            a.chain_id as chain_id,
            a.evo_chain_id as branch_number,
            a.stage as stage,
            a.stage_number as stage_name,
            b.dex_number as species_id

        from pivot_name a

        inner join
            pivot_dex b
            on a.chain_id = b.chain_id
            and a.evo_chain_id = b.evo_chain_id
            and a.stage = b.dex

    )

-- Simple Select Statement
select *
from clean_evolution_chain
