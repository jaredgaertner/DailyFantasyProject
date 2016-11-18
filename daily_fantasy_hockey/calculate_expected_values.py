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

def update_line_combinations(db, force_update = False):

    try:
        # Check if we've updated in the last 12 hours
        twelve_hours_ago = datetime.datetime.today() - datetime.timedelta(hours=12)
        db.query("SELECT EXISTS(SELECT 1 FROM player_line_combinations WHERE updatedOn > ?) recentlyUpdated", (twelve_hours_ago,))
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
                        if position.startswith(("C","LW","RW","LD","RD","G","IR")) and td.a != None:
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

        db.query("select t.id from teams t where t.name = ? or t.teamName = ?", (visiting_name, visiting_name.split()[1]))
        for team in db.fetchall():
            away_team_id = team['id']


        # Couldn't find the player, try their last name only
        db.query("select g.gamePk from games g where homeTeamId = ? and awayTeamId = ?", (home_team_id, away_team_id))
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
                home_moneyline = event.periods.period.moneyline.moneyline_home.string # Find first moneyline, as second contains odds for 1st period
                home_ip = get_implied_probability(int(home_moneyline))
            elif participant.visiting_home_draw.string == "Visiting":
                visiting_name = participant.participant_name.string
                visiting_moneyline = event.moneyline.moneyline_visiting.string
                visiting_ip = get_implied_probability(int(visiting_moneyline))
            else:
                raise ValueError("Unexpected visiting_home_draw string.")

        if home_name == "Home Goals":
            break

        total_p = home_ip + visiting_ip
        home_p = home_ip / total_p
        visiting_p = visiting_ip / total_p

        # Find first total, as second contains odds for 1st period
        total_points = event.periods.period.total.total_points.string
        over_adjust = event.periods.period.total.over_adjust.string
        under_adjust = event.periods.period.total.under_adjust.string

        logging.info("Home name: " + home_name)
        logging.info("Visiting name: " + visiting_name)
        logging.info("Home moneyline: " + home_moneyline)
        logging.info("Visiting moneyline: " + visiting_moneyline)
        logging.info("Home probability: " + str(home_p))
        logging.info("Visiting probability: " + str(visiting_p))
        logging.info("Total points (goals): " + str(total_points))
        logging.info(over_adjust)
        logging.info(under_adjust)

        gamePk = get_game_pk_by_names(db, home_name, visiting_name)

        db.query('''insert or replace into games_vegas_odds
                 (gamePk,
                  homeMoneyline,
                  visitingMoneyline,
                  homeProbability,
                  visitingProbability,
                  totalPoints,
                  updatedOn) VALUES(?,?,?,?,?,?,?)''',
                 (gamePk, home_moneyline, visiting_moneyline, home_p, visiting_p, total_points, datetime.datetime.today()))


def get_expected_goals(playerGame):
    return 0

def get_expected_assists(playerGame):
    return 0

def get_expected_shots_on_goal(playerGame):
    return 0

def get_expected_blocked_shots(playerGame):
    return 0

def get_expected_short_handed_points(playerGame):
    return 0

def get_expected_shootout_goals(playerGame):
    return 0

def get_expected_hat_tricks(playerGame):
    return 0

def get_expected_wins(playerGame):
    return 0

def get_expected_saves(playerGame):
    return 0

def get_expected_goals_against(playerGame):
    return 0

def get_expected_shutouts(playerGame):
    return 0

def update_expected_values(db):
    active_players = get_all_active_player_ids(db)
    for active_player_id in active_players:

        playerGame = PlayerGame(db, active_player_id)
        expectedStatsList = [playerGame.get_player_id(),
                             playerGame.get_game_pk(),
                             get_expected_goals(playerGame),
                             get_expected_assists(playerGame),
                             get_expected_shots_on_goal(playerGame),
                             get_expected_blocked_shots(playerGame),
                             get_expected_short_handed_points(playerGame),
                             get_expected_shootout_goals(playerGame),
                             get_expected_hat_tricks(playerGame),
                             get_expected_wins(playerGame),
                             get_expected_saves(playerGame),
                             get_expected_goals_against(playerGame),
                             get_expected_shutouts(playerGame),
                             datetime.datetime.today()]
        db.query('''insert or replace into player_games_expected_stats
                     (playerId integer,
                      gamePk integer,
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
                      updatedOn date), values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', expectedStatsList)

def calculate_expected_values(db):

    # Update team stats
    update_team_stats(db, "20162017")

    # Update line combinations
    update_line_combinations(db)

    # Update vegas lines
    update_games_vegas_odds(db)

    update_expected_values(db)

    db.commit()