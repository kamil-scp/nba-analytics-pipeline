select
    game_id,
    season_id,
    game_date,
    player_id,
    team_id,
    home_away,
    pts,
    reb,
    ast,
    min,
    plus_minus,
    case when pts >= 30 then 1 else 0 end as is_30_plus
from {{ ref('int_player_game_stats') }}
