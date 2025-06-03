-- With statement
with

    -- Import CTEs
    base_type as (select * from {{ ref("stg_pokemon__type") }}),

    -- Logical CTEs
    all_type_combinations as (

        select

            a.raw_json:id::int as type_id,
            initcap(a.raw_json:name::string) as type_name,
            initcap(b.raw_json:name::string) as attack_type,
            null::float as damage

        from base_type a, base_type b

    ),

    no_damage as (

        select

            raw_json:id::int as type_id,
            initcap(raw_json:name::string) as type_name,
            initcap(damage0.value:name::string) as attack_type,
            0.0::float as damage

        from
            base_type,
            lateral flatten(
                input => raw_json:damage_relations:no_damage_from
            ) as damage0

    ),

    half_damage as (

        select

            raw_json:id::int as type_id,
            initcap(raw_json:name::string) as type_name,
            initcap(damage05.value:name::string) as attack_type,
            0.5 as damage

        from
            base_type,
            lateral flatten(
                input => raw_json:damage_relations:half_damage_from
            ) as damage05

    ),

    double_damage as (

        select

            raw_json:id::int as type_id,
            initcap(raw_json:name::string) as type_name,
            initcap(damage2.value:name::string) as attack_type,
            2::float as damage

        from
            base_type,
            lateral flatten(
                input => raw_json:damage_relations:double_damage_from
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

    main_table as (

        select type_name, attack_type, ifnull(max(damage), 1) as damage

        from union_tables

        where
            attack_type not in ('Shadow', 'Stellar', 'Unknown')
            and type_name not in ('Shadow', 'Stellar', 'Unknown')

        group by type_id, type_name, attack_type

    ),

    clean_type_matchups as (

        select
            t1.type_name || '-' || t2.type_name as type_name,
            t1.attack_type,
            t1.damage * t2.damage as damage

        from main_table t1

        join
            main_table t2
            on t1.attack_type = t2.attack_type
            and t1.type_name != t2.type_name

        union all
        select *
        from main_table

    )

-- Simple Select Statement
select *
from clean_type_matchups
