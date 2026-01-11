{{ config(
    materialized='table'
) }}

select
    s.season_label as season,
    g.team_id,
    g.team_name,

    avg(g.points)      as avg_points,
    avg(g.rebounds)    as avg_rebounds,
    avg(g.assists)     as avg_assists,
    avg(g.fg_pct)      as avg_fg_pct,
    avg(g.plus_minus)  as avg_plus_minus,

    count(distinct game_id) as games_played

from {{ ref('games_union') }} g
join {{ source('nba','seasons') }} s
    on g.season_id = s.season_id
group by 1,2,3
