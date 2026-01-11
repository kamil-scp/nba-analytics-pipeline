select
    season_id,
    player_id,
    count(distinct game_id) as games_played,
    avg(pts) as avg_points,
    avg(reb) as avg_rebounds,
    avg(ast) as avg_assists,
    sum(is_30_plus) as games_30_plus
from {{ ref('fct_player_games') }}
group by season_id, player_id
