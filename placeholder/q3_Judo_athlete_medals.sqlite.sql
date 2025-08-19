with judo_medal_athletes as (
    select athletes.name, athletes.code 
        from medals 
    join athletes on medals.winner_code = athletes.code 
    and medals.discipline = 'Judo'
), athlete_teams as (
    select teams.*
        from judo_medal_athletes, teams  
    where  teams.athletes_code = judo_medal_athletes.code
), team_medals as (
    select athlete_teams.code, athlete_teams.athletes_code
        from medals, athlete_teams
    where medals.winner_code = athlete_teams.code
), all_medals as (
    select judo_medal_athletes.code as code
    from judo_medal_athletes
    union all
    select team_medals.athletes_code 
    from team_medals
) select athletes.name, count(*) as medal_amount
    from all_medals, athletes
    where all_medals.code = athletes.code
group by all_medals.code
order by medal_amount desc, name asc;


