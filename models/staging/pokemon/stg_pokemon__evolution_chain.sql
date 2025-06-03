select *
from {{ source('pokemon', 'pokemon_chain_json_table') }}