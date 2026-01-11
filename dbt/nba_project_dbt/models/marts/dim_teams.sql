select
    t.id as team_id,
    t.full_name as team_name,
    t.abbreviation,
    t.city,
    t.state,
    t.year_founded
from {{ source('nba','teams') }} t
