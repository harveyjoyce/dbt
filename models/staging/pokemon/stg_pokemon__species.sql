select *
from {{ source('pokemon', 'pokemon_species_json_table') }}