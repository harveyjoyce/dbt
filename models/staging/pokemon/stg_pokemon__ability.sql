select *
from {{ source('pokemon', 'pokemon_ability_json_table') }}