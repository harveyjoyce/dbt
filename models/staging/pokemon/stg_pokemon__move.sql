select *
from {{ source('pokemon', 'pokemon_move_json_table') }}