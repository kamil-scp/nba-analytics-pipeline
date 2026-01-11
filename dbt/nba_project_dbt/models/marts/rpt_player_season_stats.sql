{{ config(
    materialized='table'
) }}

with player_games as (

    select
        game_id,
        player_id,
        team_id,
        pts as points,
        reb as rebounds,
        ast as assists
    from {{ ref('fct_player_games') }}
),

games as (

    select distinct
        game_id,
        season_id
    from {{ ref('games_union') }}

)

select
    s.season_label as season,
    pg.player_id,
    p.full_name as player_name,
    pg.team_id,
    t.team_name,

    sum(pg.points)   as total_points,
    sum(pg.rebounds) as total_rebounds,
    sum(pg.assists)  as total_assists,

    avg(pg.points)   as avg_points,
    avg(pg.rebounds) as avg_rebounds,
    avg(pg.assists)  as avg_assists,

    count(distinct pg.game_id) as games_played

from player_games pg
join games g
    on pg.game_id = g.game_id
join {{ ref('dim_players') }} p
    on pg.player_id = p.player_id
join {{ ref('dim_teams') }} t
    on pg.team_id = t.team_id
join {{ source('nba','seasons') }} s
    on g.season_id = s.season_id

group by 1,2,3,4,5
