select game_id
from {{ ref('fct_player_games') }}
group by game_id
having count(distinct home_away) != 2