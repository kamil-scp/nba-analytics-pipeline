select
    pf.game_id,
    g.season_id,
    g.game_date,
    pf.player_id,
    pf.team_id,
    g.home_away,
    pf.pts,
    pf.reb,
    pf.ast,
    cast(split(pf.min, ':')[offset(0)] as float64)
      + cast(split(pf.min, ':')[offset(1)] as float64)/60 as min,
    pf.plus_minus
from {{ ref('players_fact') }} pf
join {{ ref('games_union') }} g
  on pf.game_id = g.game_id
 and pf.team_id = g.team_id
 and pf.pts is not null
