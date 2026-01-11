select
    g.game_id,
    g.team_id,
    abs(sum(p.pts) - avg(g.points)) as diff
from {{ ref('fct_player_games') }} p
join {{ ref('games_union') }} g
  on p.game_id = g.game_id
 and p.team_id = g.team_id
group by g.game_id, g.team_id
having diff > 5
