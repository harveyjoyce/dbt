select *
from {{ source('pokemon', 'pokemon_json_table') }}