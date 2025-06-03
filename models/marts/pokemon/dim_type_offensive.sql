-- With statement
with

    -- Import CTEs
    base_type as (select * from {{ ref("stg_pokemon__type") }}),

    -- Logical CTEs
    all_type_combinations as (

        select

            a.raw_json:id::int as type_id,
            initcap(a.raw_json:name::string) as type_name,
            initcap(b.raw_json:name::string) as defend_type,
            null::float as damage

        from base_type a, base_type b

    ),

    no_damage as (

        select

            raw_json:id::int as type_id,
            initcap(raw_json:name::string) as type_name,
            initcap(damage0.value:name::string) as defend_type,
            0.0::float as damage

        from
            base_type,
            lateral flatten(input => raw_json:damage_relations:no_damage_to) as damage0

    ),

    half_damage as (

        select

            raw_json:id::int as type_id,
            initcap(raw_json:name::string) as type_name,
            initcap(damage05.value:name::string) as defend_type,
            0.5 as damage

        from
            base_type,
            lateral flatten(
                input => raw_json:damage_relations:half_damage_to
            ) as damage05

    ),

    double_damage as (

        select

            raw_json:id::int as type_id,
            initcap(raw_json:name::string) as type_name,
            initcap(damage2.value:name::string) as defend_type,
            2::float as damage

        from
            base_type,
            lateral flatten(
                input => raw_json:damage_relations:double_damage_to
            ) as damage2

    ),

    union_tables as (

        select *
        from all_type_combinations
        union all
        select *
        from no_damage
        union all
        select *
        from half_damage
        union all
        select *
        from double_damage

    ),

    clean_type_matchups as (

        select

            type_id as type_id,
            type_name as type_name,
            defend_type as defend_type,
            ifnull(max(damage), 1) as damage

        from union_tables

        where
            defend_type not in ('Shadow', 'Stellar', 'Unknown')
            and type_name not in ('Shadow', 'Stellar', 'Unknown')

        group by type_id, type_name, defend_type

        order by type_id, defend_type

    )

-- Simple Select Statement
select *
from clean_type_matchups
