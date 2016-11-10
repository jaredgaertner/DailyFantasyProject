select id, name, weight, value, value/weight, position, gameInfo
from player_draftkings_info
where weight <= 90 and position != "G" and
      date(gameDate) = date('now')
order by value/weight desc