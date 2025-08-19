with medal_country as (
select medals.*, athletes.country_code 
from medals 
join athletes on medals.winner_code = athletes.code 
union 
select medals.*, teams.country_code 
from medals 
join teams on medals.winner_code = teams.code)

select name as coach_name, count(*) as medal_amount
    from coaches, medal_country 
    where 
        medal_country.discipline = coaches.discipline 
        and medal_country.country_code = coaches.country_code

group by name
order by medal_amount desc, coach_name asc;