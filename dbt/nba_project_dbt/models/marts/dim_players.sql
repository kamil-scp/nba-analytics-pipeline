select
    p.id as player_id,
    p.full_name,
    p.first_name,
    p.last_name,
    p.is_active
from {{ source('nba','players') }} p
