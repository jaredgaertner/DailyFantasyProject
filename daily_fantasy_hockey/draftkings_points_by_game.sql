select gamePk, points, fullName
from games_draftkings_points gdp
inner join players p 
on gdp.playerid = p.id
order by gamePk desc;