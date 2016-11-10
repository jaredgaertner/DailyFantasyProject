select ddl.gameDate,
       (select p.fullName
	     from players p
		 where p.id = ddl.centre1) centre1,
       (select p.fullName
	     from players p
		 where p.id = ddl.centre2) centre2,
       (select p.fullName
	     from players p
		 where p.id = ddl.winger1) winger1,
       (select p.fullName
	     from players p
		 where p.id = ddl.winger2) winger2,
       (select p.fullName
	     from players p
		 where p.id = ddl.winger3) winger3,
       (select p.fullName
	     from players p
		 where p.id = ddl.defence1) defence1,
       (select p.fullName
	     from players p
		 where p.id = ddl.defence2) defence2,
       (select p.fullName
	     from players p
		 where p.id = ddl.goalie) goalie,
       (select p.fullName
	     from players p
		 where p.id = ddl.util) util,
       ddl.totalWeight,
	   ddl.totalValue,
	   ddl.actualValue  
from daily_draftkings_lineups ddl