select
    game_id,
    CAST(SUBSTR(CAST(season_id_ag AS STRING), 2) AS INTEGER) as season_id,
    team_id_ag as team_id,
    team_name_ag as team_name,
    game_date_ag as game_date,
    'AWAY' as home_away,
    pts_ag as points,
    reb_ag as rebounds,
    ast_ag as assists,
    fg_pct_ag as fg_pct,
    plus_minus_ag as plus_minus
from {{ source('nba','games') }}

union all

select
    game_id,
    CAST(SUBSTR(CAST(season_id_hg AS STRING), 2) AS INTEGER) as season_id,
    team_id_hg as team_id,
    team_name_hg as team_name,
    game_date_hg as game_date,
    'HOME' as home_away,
    pts_hg as points,
    reb_hg as rebounds,
    ast_hg as assists,
    fg_pct_hg as fg_pct,
    plus_minus_hg as plus_minus
from {{ source('nba','games') }}
