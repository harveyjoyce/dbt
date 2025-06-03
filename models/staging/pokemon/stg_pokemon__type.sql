select *
from {{ source('pokemon', 'pokemon_type_json_table') }}