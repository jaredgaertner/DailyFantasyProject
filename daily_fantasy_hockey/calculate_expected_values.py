import json
import sqlite3
import datetime
import urllib.request
import time
import logging
from database import database
from bs4 import BeautifulSoup
from player_game import PlayerGame

__author__ = "jaredg"


def drop_tables(db):
    try:
        db.query('''DROP table team_stats''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)

    try:
        db.query('''DROP table player_line_combinations''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)


def create_team_stats(db):
    # Create team stats table
    # Based on NHL API: http://www.nhl.com/stats/rest/grouped/team/basic/season/teamsummary?cayenneExp=seasonId=20162017%20and%20gameTypeId=2
    db.query('''CREATE TABLE team_stats
                 (teamId number primary key,
                  seasonId number,
                  gamesPlayed number,
                  wins number,
                  ties text,
                  losses number,
                  otLosses number,
                  points number,
                  regPlusOtWins number,
                  pointPctg number,
                  goalsFor number,
                  goalsAgainst number,
                  goalsForPerGame number,
                  goalsAgainstPerGame number,
                  ppPctg number,
                  pkPctg number,
                  shotsForPerGame number,
                  shotsAgainstPerGame number,
                  faceoffWinPctg number,
                  goalsAgainstPerGamePercentage number,
                  foreign key(teamId) references teams(id))''')


def create_player_line_combinations(db):
    db.query('''CREATE TABLE player_line_combinations
                 (playerId integer primary key,
                  lineInfo text,
                  createdOn date,
                  updatedOn date,
                  foreign key (playerId) references players(id))''')


def create_games_vegas_odds(db):
    db.query('''CREATE TABLE games_vegas_odds
                 (gamePk integer primary key,
                  homeMoneyline text,
                  visitingMoneyline text,
                  homeProbability number,
                  visitingProbability number,
                  totalPoints,
                  updatedOn date,
                  foreign key (gamePk) references games(gamePk))''')


def create_player_games_expected_stats(db):
    # Create player games expected stats table
    db.query('''CREATE TABLE player_games_expected_stats
                 (playerId integer,
                  gamePk integer,
                  opponentId integer,
                  goals number,
                  assists number,
                  shotsOnGoal number,
                  blockedShots number,
                  shortHandedPoints number,
                  shootoutGoals number,
                  hatTricks number,
                  wins number,
                  saves number,
                  goalsAgainst number,
                  shutouts number,
                  updatedOn date,
                  primary key (playerId, gamePk),
                  foreign key(playerId) references players(id),
                  foreign key(gamePk) references games(gamePk),
                  foreign key(opponentId) references teams(id))''')


def create_tables(db):
    # Create lineup combinations
    create_player_line_combinations(db)

    # Create team stats table
    # Based on NHL API: http://www.nhl.com/stats/rest/grouped/team/basic/season/teamsummary?cayenneExp=seasonId=20162017%20and%20gameTypeId=2
    create_team_stats(db)

    create_games_vegas_odds(db)

    create_player_games_expected_stats(db)


def update_games_draftkings_points(db, update_date):
    # Draftkings point system:
    # Players will accumulate points as follows:
    #     Goal = +3 PTS
    #     Assist = +2 PTS
    #     Shot on Goal = +0.5 PTS
    #     Blocked Shot = +0.5 PTS
    #     Short Handed Point Bonus (Goal/Assist) = +1 PTS
    #     Shootout Goal = +0.2 PTS
    #     Hat Trick Bonus = +1.5 PTS
    #
    # Goalies only will accumulate points as follows:
    #     Win = +3 PTS
    #     Save = +0.2 PTS
    #     Goal Against = -1 PTS
    #     Shutout Bonus = +2 PTS
    #     Goalie Scoring Notes:
    #     Goalies WILL receive points for all stats they accrue, including goals and assists.
    #     The Goalie Shutout Bonus is credited to goalies if they complete the entire game with 0 goals allowed in regulation + overtime. Shootout goals will not prevent a shutout. Goalie must complete the entire game to get credit for a shutout.

    # Create points data
    # Player stats
    db.query('''SELECT gpss.*
                FROM games g
                LEFT JOIN games_players_skater_stats gpss ON g.gamePk = gpss.gamePk
                WHERE g.gameDate > ?''', (update_date,))
    for player_stats in db.fetchall():
        try:
            gamePk = player_stats['gamePk']
            playerId = player_stats['playerId']
            points = player_stats["goals"] * 3 + player_stats["assists"] * 2 + player_stats["shots"] * 0.5 + \
                     player_stats["blocked"] * 0.5 + player_stats["shortHandedGoals"] + player_stats[
                         "shortHandedAssists"]  # + player_stats["Shootout"] * 0.2
            if player_stats["goals"] >= 3:
                points += 1.5

            logging.info("Updating player " + str(playerId) + " draftkings stats for game ID: " + str(gamePk))
            db.query('''INSERT or REPLACE INTO games_draftkings_points
                     (gamePk,
                      playerId,
                      points) VALUES(?,?,?)''', (gamePk, playerId, points))

        except Exception as e:
            logging.error("Could not insert the following player points for DraftKings:")
            logging.error(player_stats)
            logging.error("Got the following error:")
            logging.error(e)
            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e

    # Goalie stats
    db.query('''SELECT gpgs.*
                FROM games g
                LEFT JOIN games_players_goalie_stats gpgs ON g.gamePk = gpgs.gamePk
                WHERE g.gameDate > ?''', (update_date,))
    for goalie_stats in db.fetchall():
        try:
            gamePk = goalie_stats['gamePk']
            playerId = goalie_stats['playerId']

            goals_against = (goalie_stats["shots"] - goalie_stats["saves"])
            points = goalie_stats["saves"] * 0.2 - goals_against + goalie_stats["goals"] * 3 + goalie_stats[
                                                                                                   "assists"] * 2
            if goalie_stats["decision"] == "W":
                points += 3

            if goals_against == 0:
                # check time on ice as well, needs to be the entire game
                # Accounts for cases where the goalie is pulled (delayed penalty)
                #  by not being quite 60 minutes
                # TODO: Fix this up, not quite correct, needs to check game length
                min, seconds = [int(i) for i in goalie_stats["timeOnIce"].split(':')]
                if (min * 60 + seconds) > 3550:
                    points += 2

            db.query('''INSERT or REPLACE INTO games_draftkings_points
                     (gamePk,
                      playerId,
                      points) VALUES(?,?,?)''', (gamePk, playerId, points))

        except Exception as e:
            logging.error("Could not insert the following goalie points for DraftKings:")
            logging.error(goalie_stats)
            logging.error("Got the following error:")
            logging.error(e)
            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e


def update_team_stats(db, season):
    # Create team data
    url = "http://www.nhl.com/stats/rest/grouped/team/basic/season/teamsummary?cayenneExp=seasonId=" + season + "%20and%20gameTypeId=2"
    response = urllib.request.urlopen(url).read()
    data = json.loads(response.decode())

    for team_stat in data['data']:
        try:
            db.query(
                '''INSERT or REPLACE INTO team_stats(teamId, seasonId, gamesPlayed, wins, ties, losses, otLosses, points, regPlusOtWins, pointPctg, goalsFor, goalsAgainst, goalsForPerGame, goalsAgainstPerGame, ppPctg, pkPctg, shotsForPerGame, shotsAgainstPerGame, faceoffWinPctg) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                [team_stat['teamId'], team_stat['seasonId'], team_stat['gamesPlayed'], team_stat['wins'],
                 team_stat['ties'], team_stat['losses'], team_stat['otLosses'], team_stat['points'],
                 team_stat['regPlusOtWins'], team_stat['pointPctg'], team_stat['goalsFor'], team_stat['goalsAgainst'],
                 team_stat['goalsForPerGame'], team_stat['goalsAgainstPerGame'], team_stat['ppPctg'],
                 team_stat['pkPctg'], team_stat['shotsForPerGame'], team_stat['shotsAgainstPerGame'],
                 team_stat['faceoffWinPctg']])

        except Exception as e:
            logging.error("Could not insert the following team stats:")
            logging.error(team_stat)
            logging.error("Got the following error:")
            raise e
            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e


def get_player_id_by_name(db, playerName):
    try:
        db.query("select p.id from players p where p.fullName = ?", (playerName,))
        for player in db.fetchall():
            return player['id']

        # Couldn't find the player, try their last name only
        db.query("select p.id from players p where lower(p.lastName) = lower(?)", (playerName.split()[1],))
        for player in db.fetchall():
            return player['id']

    except Exception as e:
        logging.error("Could not find player ID for " + playerName)
        logging.error("Got the following error:")
        logging.error(e)
        raise e


def get_all_active_players(db):
    player_ids = []

    try:
        db.query("select distinct playerId from player_line_combinations where lineInfo not like 'IR%'")
        for player in db.fetchall():
            player_ids.append(player['playerId'])
        return player_ids

    except Exception as e:
        logging.error("Could not connect to dailyfaceoff to get lineup combinations.")
        logging.error("Got the following error:")
        logging.error(e)
        # Roll back any change if something goes wrong
        # db.rollback()
        raise e


def update_line_combinations(db, force_update=False):
    try:
        # Check if we've updated in the last 12 hours
        twelve_hours_ago = datetime.datetime.today() - datetime.timedelta(hours=12)
        db.query("SELECT EXISTS(SELECT 1 FROM player_line_combinations WHERE updatedOn > ?) recentlyUpdated",
                 (twelve_hours_ago,))
        if db.fetchone()['recentlyUpdated'] == 1 and force_update != True:
            logging.info("Skipping updating lineup combinations, recently updated....")
        else:
            logging.info("Finding line combinations...")
            url = "http://www2.dailyfaceoff.com/teams"
            soup = BeautifulSoup(urllib.request.urlopen(url).read(), "lxml")
            teams = soup.find(id="matchups_container")
            for team in teams.find_all("a"):
                url = team.get("href")
                if url.startswith("/teams"):
                    url = "http://www2.dailyfaceoff.com" + team.get("href")
                    soup = BeautifulSoup(urllib.request.urlopen(url).read(), "html.parser")
                    lineups = soup.find(id="matchups_container")
                    for td in lineups.find_all("td"):
                        logging.debug(td)
                        # Going to ignore powerplay lineups for now
                        position = td.get("id")
                        logging.debug(position)
                        if position.startswith(("C", "LW", "RW", "LD", "RD", "G", "IR")) and td.a != None:
                            playerName = td.a.img.get("alt")
                            logging.info("Setting " + str(playerName) + " to " + position)
                            playerId = get_player_id_by_name(db, playerName)
                            db.query('''INSERT or REPLACE INTO player_line_combinations
                                     (playerId,
                                      lineInfo,
                                      updatedOn) VALUES(?,?,?)''', (playerId, position, datetime.datetime.today()))
                        else:
                            logging.debug("ignoring..." + str(position))


    except Exception as e:
        logging.error("Could not connect to dailyfaceoff to get lineup combinations.")
        logging.error("Got the following error:")
        logging.error(e)
        # Roll back any change if something goes wrong
        db.rollback()
        raise e


def get_implied_probability(american_odds):
    if american_odds < 0:
        positive_odds = -american_odds
        return (positive_odds) / (positive_odds + 100)
    else:
        return 100 / (american_odds + 100)


def get_game_pk_by_names(db, home_name, visiting_name):
    try:
        db.query("select t.id from teams t where t.name = ? or t.teamName = ?", (home_name, home_name.split()[1]))
        for team in db.fetchall():
            home_team_id = team['id']

        db.query("select t.id from teams t where t.name = ? or t.teamName = ?",
                 (visiting_name, visiting_name.split()[1]))
        for team in db.fetchall():
            away_team_id = team['id']

        # Find the latest gamePk with the given team names
        db.query("select g.gamePk from games g where homeTeamId = ? and awayTeamId = ? order by gamePk desc", (home_team_id, away_team_id))
        for game in db.fetchall():
            return game['gamePk']

    except Exception as e:
        logging.error("Could not find gamePk for " + home_name + ", " + visiting_name)
        logging.error("Got the following error:")
        logging.error(e)
        raise e


def get_all_active_player_ids(db):
    player_ids = []

    try:
        db.query("select distinct playerId from player_line_combinations where lineInfo not like 'IR%'")
        for player in db.fetchall():
            player_ids.append(player['playerId'])
        return player_ids

    except Exception as e:
        logging.error("Could not connect to dailyfaceoff to get lineup combinations.")
        logging.error("Got the following error:")
        logging.error(e)
        # Roll back any change if something goes wrong
        # db.rollback()
        raise e


def update_games_vegas_odds(db):
    # Check pinnacle for odds (use sportsubtype=Live%20NHL for in progress games)
    url = "http://xml.pinnaclesports.com/pinnaclefeed.aspx?sporttype=Hockey&sportsubtype=NHL%20OT%20Incl"
    soup = BeautifulSoup(urllib.request.urlopen(url).read(), "lxml-xml")
    for event in soup.find_all("event"):
        for participant in event.find_all("participant"):

            if participant.visiting_home_draw.string == "Home":
                home_name = participant.participant_name.string
            elif participant.visiting_home_draw.string == "Visiting":
                visiting_name = participant.participant_name.string
            else:
                raise ValueError("Unexpected visiting_home_draw string.")

        # Check for the total home/away goals bet and exit
        if home_name == "Home Goals" or visiting_name == 'Away Goals':
            continue

        # Check for games which don't have odds yet
        if event.periods.find("period") == None:
            logging.info("No moneyline information, skipping for " + home_name + ", " + visiting_name)
            continue

        # Find first moneyline, as second contains odds for 1st period
        home_moneyline = event.periods.period.moneyline.moneyline_home.string
        home_ip = get_implied_probability(int(home_moneyline))
        visiting_moneyline = event.moneyline.moneyline_visiting.string
        visiting_ip = get_implied_probability(int(visiting_moneyline))
        total_p = home_ip + visiting_ip
        home_p = home_ip / total_p
        visiting_p = visiting_ip / total_p

        # Find first total, as second contains odds for 1st period
        total_points = event.periods.period.total.total_points.string
        over_adjust = event.periods.period.total.over_adjust.string
        under_adjust = event.periods.period.total.under_adjust.string

        logging.debug("Home name: " + home_name)
        logging.debug("Visiting name: " + visiting_name)
        logging.debug("Home moneyline: " + home_moneyline)
        logging.debug("Visiting moneyline: " + visiting_moneyline)
        logging.debug("Home probability: " + str(home_p))
        logging.debug("Visiting probability: " + str(visiting_p))
        logging.debug("Total points (goals): " + str(total_points))
        logging.debug(over_adjust)
        logging.debug(under_adjust)

        gamePk = get_game_pk_by_names(db, home_name, visiting_name)

        db.query('''insert or replace into games_vegas_odds
                 (gamePk,
                  homeMoneyline,
                  visitingMoneyline,
                  homeProbability,
                  visitingProbability,
                  totalPoints,
                  updatedOn) VALUES(?,?,?,?,?,?,?)''',
                 (gamePk, home_moneyline, visiting_moneyline, home_p, visiting_p, total_points,
                  datetime.datetime.today()))


def get_expected_skater_stats(db, playerGame):
    skater_stats = {"goals": 0,
                    "assists": 0,
                    "shots_on_goal": 0,
                    "blocked_shots": 0,
                    "short_handed_points": 0,
                    "shootout_goals": 0,
                    "hat_tricks": 0}

    if playerGame.get_primary_position() == "G":
        return skater_stats

    playerId = playerGame.get_player_id()
    try:
        logging.debug("Getting player value for " + str(playerId))
        # Find average points for last week and for the year
        db.query('''select ifnull(avg(case when g.gamePk like '2015%' then pgss.goals else null end),0) AS average_goals_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then pgss.goals else null end),0) AS average_goals_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then pgss.goals else null end),0) AS average_goals_last_two_weeks,
                           ifnull(avg(case when g.gamePk like '2015%' then pgss.assists else null end),0) AS average_assists_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then pgss.assists else null end),0) AS average_assists_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then pgss.assists else null end),0) AS average_assists_last_two_weeks,
                           ifnull(avg(case when g.gamePk like '2015%' then pgss.shots else null end),0) AS average_shots_on_goal_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then pgss.shots else null end),0) AS average_shots_on_goal_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then pgss.shots else null end),0) AS average_shots_on_goal_last_two_weeks,
                           ifnull(avg(case when g.gamePk like '2015%' then pgss.blocked else null end),0) AS average_blocked_shots_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then pgss.blocked else null end),0) AS average_blocked_shots_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then pgss.blocked else null end),0) AS average_blocked_shots_last_two_weeks,
                           ifnull(avg(case when g.gamePk like '2015%' then pgss.shortHandedGoals + pgss.shortHandedAssists else null end),0) AS average_short_handed_points_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then pgss.shortHandedGoals + pgss.shortHandedAssists else null end),0) AS average_short_handed_points_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then pgss.shortHandedGoals + pgss.shortHandedAssists else null end),0) AS average_short_handed_points_last_two_weeks,
                           0 average_shootout_goals_last_year,
                           0 average_shootout_goals_this_year,
                           0 average_shootout_goals_last_two_weeks,
                           ifnull(avg(case when g.gamePk like '2015%' then (case when pgss.goals >= 3 then 1 else 0 end) else null end),0) AS average_hat_tricks_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then (case when pgss.goals >= 3 then 1 else 0 end) else null end),0) AS average_hat_tricks_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then (case when pgss.goals >= 3 then 1 else 0 end) else null end),0) AS average_hat_tricks_last_two_weeks,
                           count(case when g.gamePk like '2015%' then 1 else null end) as games_last_year,
                           count(case when g.gamePk like '2016%' then 1 else null end) as games_this_year,
                           count(case when g.gameDate > date('now','-14 day') then 1 else null end) AS games_last_two_weeks
                    from player_games_skater_stats pgss
                    inner join games g
                    on pgss.gamePk = g.gamePk
                    where playerId = ? and
                          (g.gamePk like '2016%' or g.gamePk like '2015%')''', (playerId,))
        value = 0
        for player_stats in db.fetchall():
            # Calculate value (ignore players that haven't played a game this year)
            if player_stats['games_this_year'] != 0:
                # Calculate total games (will be over one due to last two weeks, but want to find the ratio for each stat)
                total_games = player_stats['games_last_year'] + player_stats['games_this_year'] + player_stats['games_last_two_weeks']
                games_last_year_ratio = player_stats['games_last_year'] / total_games
                games_this_year_ratio = player_stats['games_this_year'] / total_games
                games_last_two_weeks_ratio = player_stats['games_last_two_weeks'] / total_games
                logging.debug("Total games last year is " + str(player_stats['games_last_year']))
                logging.debug("Total games this year is " + str(player_stats['games_this_year']))
                logging.debug("Total games last two weeks is " + str(player_stats['games_last_two_weeks']))

                # Loop through and get an adjusted value based on each time interval
                for key, value in skater_stats.items():
                    skater_stats[key] = games_last_year_ratio * player_stats['average_' + key + '_last_year'] + \
                                        games_this_year_ratio * player_stats['average_' + key + '_this_year'] + \
                                        games_last_two_weeks_ratio * player_stats['average_' + key + '_last_two_weeks']
                    logging.debug("For player id " + str(playerId) + ": " + key + " = " + str(skater_stats[key]) + "")

        return skater_stats

    except Exception as e:
        logging.error("Could not get skater expected stats.")
        logging.error("Got the following error:")
        logging.error(e)
        # Roll back any change if something goes wrong
        # db.rollback()
        raise e


def get_expected_goalie_stats(db, playerGame):
    goalie_stats = {"wins": 0,
                    "saves": 0,
                    "goals_against": 0,
                    "shutouts": 0,
                    "goals" : 0,
                    "assists" : 0}

    if playerGame.get_primary_position() != "G":
        return goalie_stats

    playerId = playerGame.get_player_id()
    try:
        logging.debug("Getting player value for " + str(playerId))
        # Find average points for last week and for the year
        db.query('''select ifnull(avg(case when g.gamePk like '2015%' then (case when pggs.decision = "W" then 1 else 0 end) else null end),0) AS average_wins_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then (case when pggs.decision = "W" then 1 else 0 end) else null end),0) AS average_wins_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then (case when pggs.decision = "W" then 1 else 0 end) else null end),0) AS average_wins_last_two_weeks,
                           ifnull(avg(case when g.gamePk like '2015%' then pggs.saves else null end),0) AS average_saves_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then pggs.saves else null end),0) AS average_saves_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then pggs.saves else null end),0) AS average_saves_last_two_weeks,
                           ifnull(avg(case when g.gamePk like '2015%' then pggs.shots - pggs.saves else null end),0) AS average_goals_against_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then pggs.shots - pggs.saves else null end),0) AS average_goals_against_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then pggs.shots - pggs.saves else null end),0) AS average_goals_against_last_two_weeks,
                           ifnull(avg(case when g.gamePk like '2015%' then (case when pggs.decision = "W" and pggs.shots = pggs.saves and pggs.timeOnIce > "59:30" then 1 else 0 end) else null end),0) AS average_shutouts_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then (case when pggs.decision = "W" and pggs.shots = pggs.saves and pggs.timeOnIce > "59:30" then 1 else 0 end) else null end),0) AS average_shutouts_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then (case when pggs.decision = "W" and pggs.shots = pggs.saves and pggs.timeOnIce > "59:30" then 1 else 0 end) else null end),0) AS average_shutouts_last_two_weeks,
                           ifnull(avg(case when g.gamePk like '2015%' then pggs.goals else null end),0) AS average_goals_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then pggs.goals else null end),0) AS average_goals_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then pggs.goals else null end),0) AS average_goals_last_two_weeks,
                           ifnull(avg(case when g.gamePk like '2015%' then pggs.assists else null end),0) AS average_assists_last_year,
                           ifnull(avg(case when g.gamePk like '2016%' then pggs.assists else null end),0) AS average_assists_this_year,
                           ifnull(avg(case when g.gameDate > date('now','-14 day') then pggs.assists else null end),0) AS average_assists_last_two_weeks,
                           count(case when g.gamePk like '2015%' then 1 else null end) as games_last_year,
                           count(case when g.gamePk like '2016%' then 1 else null end) as games_this_year,
                           count(case when g.gameDate > date('now','-14 day') then 1 else null end) AS games_last_two_weeks
                    from player_games_goalie_stats pggs
                    inner join games g
                    on pggs.gamePk = g.gamePk
                    where pggs.playerId = ? and
                          (g.gamePk like '2016%' or g.gamePk like '2015%')''', (playerId,))
        value = 0
        for player_stats in db.fetchall():
            # Calculate value (ignore players that haven't played a game this year)
            if player_stats['games_this_year'] != 0:
                # Calculate total games (will be over one due to last two weeks, but want to find the ratio for each stat)
                total_games = player_stats['games_last_year'] + player_stats['games_this_year'] + player_stats['games_last_two_weeks']
                games_last_year_ratio = player_stats['games_last_year'] / total_games
                games_this_year_ratio = player_stats['games_this_year'] / total_games
                games_last_two_weeks_ratio = player_stats['games_last_two_weeks'] / total_games
                logging.debug("Total games last year is " + str(player_stats['games_last_year']))
                logging.debug("Total games this year is " + str(player_stats['games_this_year']))
                logging.debug("Total games last two weeks is " + str(player_stats['games_last_two_weeks']))

                # Loop through and get an adjusted value based on each time interval
                for key, value in goalie_stats.items():
                    goalie_stats[key] = games_last_year_ratio * player_stats['average_' + key + '_last_year'] + \
                                        games_this_year_ratio * player_stats['average_' + key + '_this_year'] + \
                                        games_last_two_weeks_ratio * player_stats['average_' + key + '_last_two_weeks']
                    logging.debug("For player id " + str(playerId) + ": " + key + " = " + str(goalie_stats[key]) + "")

        return goalie_stats

    except Exception as e:
        logging.error("Could not get goalie expected stats.")
        logging.error("Got the following error:")
        logging.error(e)
        # Roll back any change if something goes wrong
        # db.rollback()
        raise e

    return goalie_stats


def update_expected_stats(db, force_update=False):
    try:
        # Check if we've updated in the last 12 hours
        twelve_hours_ago = datetime.datetime.today() - datetime.timedelta(hours=12)
        db.query("SELECT EXISTS(SELECT 1 FROM player_games_expected_stats WHERE updatedOn > ?) recentlyUpdated",
                 (twelve_hours_ago,))
        if db.fetchone()['recentlyUpdated'] == 1 and force_update != True:
            logging.info("Skipping updating expected stats, recently updated....")
        else:
            active_players = get_all_active_player_ids(db)
            for active_player_id in active_players:
                playerGame = PlayerGame(db, active_player_id)
                skater_stats = get_expected_skater_stats(db, playerGame)
                goalie_stats = get_expected_goalie_stats(db, playerGame)

                if playerGame.get_primary_position() == "G":
                    goals = goalie_stats['goals']
                    assists = goalie_stats['assists']
                else:
                    goals = skater_stats['goals']
                    assists = skater_stats['assists']

                expectedStatsList = [playerGame.get_player_id(),
                                     playerGame.get_game_pk(),
                                     playerGame.get_opponent_id(),
                                     goals,
                                     assists,
                                     skater_stats['shots_on_goal'],
                                     skater_stats['blocked_shots'],
                                     skater_stats['short_handed_points'],
                                     skater_stats['shootout_goals'],
                                     skater_stats['hat_tricks'],
                                     goalie_stats['wins'],
                                     goalie_stats['saves'],
                                     goalie_stats['goals_against'],
                                     goalie_stats['shutouts'],
                                     datetime.datetime.today()]
                db.query('''insert or replace into player_games_expected_stats
                                 (playerId,
                                  gamePk,
                                  opponentId,
                                  goals,
                                  assists,
                                  shotsOnGoal,
                                  blockedShots,
                                  shortHandedPoints,
                                  shootoutGoals,
                                  hatTricks,
                                  wins,
                                  saves,
                                  goalsAgainst,
                                  shutouts,
                                  updatedOn) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', expectedStatsList)

    except Exception as e:
        logging.error("Could not update expected stats.")
        logging.error("Got the following error:")
        logging.error(e)
        # Roll back any change if something goes wrong
        # db.rollback()
        raise e


def calculate_expected_values(db):
    # Update team stats
    update_team_stats(db, "20162017")

    # Update line combinations
    update_line_combinations(db)

    # Update vegas lines
    update_games_vegas_odds(db)

    update_expected_stats(db, True)

    db.commit()
