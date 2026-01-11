select player_id, season_id, count(*)
from {{ ref('fct_player_season') }}
group by player_id, season_id
having count(*) > 1
