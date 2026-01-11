select
    season,
    player_id,
    team_id,
    count(*) as cnt
from {{ ref('rpt_player_season_stats') }}
group by 1,2,3
having cnt > 1
