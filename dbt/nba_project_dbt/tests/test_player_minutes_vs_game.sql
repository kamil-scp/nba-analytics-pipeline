select *
from {{ ref('fct_player_games') }} p
join {{ ref('games_union') }} g
  on p.game_id = g.game_id
 and p.team_id = g.team_id
where p.min > 48