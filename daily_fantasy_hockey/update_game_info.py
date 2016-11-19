import json
import sqlite3
import datetime
import urllib.request
import time
import logging
from database import database
from bs4 import BeautifulSoup

__author__ = "jaredg"

def drop_tables(db):
    try:
        db.query('''DROP table games_draftkings_points''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)

    try:
        db.query('''DROP table players''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)

    try:
        db.query('''DROP table player_games_goalie_stats''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)

    try:
        db.query('''DROP table player_games_skater_stats''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)

    try:
        db.query('''DROP table games''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)

    try:
        db.query('''DROP table teams''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)

    try:
        db.query('''DROP table team_stats''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)


    try:
        db.query('''DROP table player_draftkings_info''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)

    try:
        db.query('''DROP table daily_draftkings_lineups''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)

    try:
        db.query('''DROP table daily_draftkings_entries''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)

    try:
        db.query('''DROP table player_game_lineup_combinations''')
    except Exception as e:
        logging.error("Could not drop tables, got the following error:")
        logging.error(e)

def create_player_draftkings_stats(db):

    # Fantasy player information for daily draftkings draft
    db.query('''CREATE TABLE player_draftkings_info
                 (id integer primary key,
                  name text,
                  nameAndId number,
                  playerId number,
                  weight number,
                  value number,
                  position text,
                  gameInfo text,
                  opponentId text,
                  gamePk number,
                  teamAbbrev text,
                  gameDate date,
                  draftType text,
                  foreign key (playerId) references players(id),
                  foreign key (gamePk) references games(gamePk)''')

def create_player_games(db):
    # Create player games table
    # Based on NHL API: https://statsapi.web.nhl.com/api/v1/game/2015020051/feed/live, under liveData -> boxScore -> teams -> away/home -> players -> IDXXXXXX -> stats -> goaliestats
    # TODO: make games_players_... reference this and any other spots that link players and games
    db.query('''CREATE TABLE player_games
                 (playerId number,
                  gamePk number,
                  opponentId number,
                  createdOn date,
                  updatedOn date,
                  primary key (playerId, gamePk),
                  foreign key(playerId) references players(id),
                  foreign key(gamePk) references games(gamePk),
                  foreign key(opponentId) references teams(id))''')

def create_daily_draftkings_lineups(db):

    # Fantasy lineup information for daily draftkings draft
    db.query('''CREATE TABLE daily_draftkings_lineups
                 (id integer primary key autoincrement,
                  centre1 integer,
                  centre2 integer,
                  winger1 integer,
                  winger2 integer,
                  winger3 integer,
                  defence1 integer,
                  defence2 integer,
                  goalie integer,
                  util integer,
                  totalWeight number,
                  totalValue number,
                  actualValue number,
                  gamePkStart integer,
                  gamePkEnd integer,
                  createdOn date,
                  updatedOn date,
                  foreign key (centre1) references players(id),
                  foreign key (centre2) references players(id),
                  foreign key (winger1) references players(id),
                  foreign key (winger2) references players(id),
                  foreign key (winger3) references players(id),
                  foreign key (defence1) references players(id),
                  foreign key (defence2) references players(id),
                  foreign key (goalie) references players(id),
                  foreign key (util) references players(id))''')

def create_daily_draftkings_entries(db):

    # Fantasy lineup information for daily draftkings draft
    db.query('''CREATE TABLE daily_draftkings_entries
                 (id integer primary key autoincrement,
                  entryId integer,
                  contestId integer,
                  contestName text,
                  entryFee text,
                  centre1 integer,
                  centre2 integer,
                  winger1 integer,
                  winger2 integer,
                  winger3 integer,
                  defence1 integer,
                  defence2 integer,
                  goalie integer,
                  util integer,
                  totalWeight number,
                  totalValue number,
                  actualValue number,
                  gamePkStart integer,
                  gamePkEnd integer,
                  createdOn date,
                  updatedOn date,
                  foreign key (centre1) references players(id),
                  foreign key (centre2) references players(id),
                  foreign key (winger1) references players(id),
                  foreign key (winger2) references players(id),
                  foreign key (winger3) references players(id),
                  foreign key (defence1) references players(id),
                  foreign key (defence2) references players(id),
                  foreign key (goalie) references players(id),
                  foreign key (util) references players(id))''')

def create_tables(db):

    # Create teams table
    # Based on NHL API: https://statsapi.web.nhl.com/api/v1/teams
    db.query('''CREATE TABLE teams
                 (id number primary key,
                  name text, link text,
                  abbreviation text,
                  teamName text,
                  locationName text,
                  firstYearOfPlay text,
                  officialSiteUrl text,
                  divisionId number,
                  conferenceId number,
                  franchiseId number,
                  shortName text,
                  active boolean)''')

    # Create games table
    # Based on NHL API: https://statsapi.web.nhl.com/api/v1/schedule?startDate=2015-10-06&endDate=2016-05-08
    db.query('''CREATE TABLE games
                 (gamePk number primary key, link text, gameType text, season number, gameDate date, statusCode number, awayTeamId number, awayScore number, homeTeamId number, homeScore number,
                 foreign key(awayTeamId) references teams(id),
                 foreign key(homeTeamId) references teams(id))''')

    # Create player table
    # Based on NHL API: http://statsapi.web.nhl.com/api/v1/people/8474688
    db.query('''CREATE TABLE players
                (id number,
                fullName text,
                link text,
                firstName text,
                lastName text,
                birthDate date,
                birthCity text,
                birthCountry text,
                height text,
                weight number,
                active boolean,
                rookie boolean,
                shootsCatches text,
                rosterStatus text,
                currentTeamId number,
                primaryPositionAbbr text,
                primary key(id),
                foreign key(currentTeamId) references teams(id))''')

    create_player_games(db)

    # Create skater stats table
    # Based on NHL API: https://statsapi.web.nhl.com/api/v1/game/2015020051/feed/live, under liveData -> boxScore -> teams -> away/home -> players -> IDXXXXXX -> stats -> skaterStats
    db.query('''CREATE TABLE player_games_skater_stats
                 (playerId number,
                  gamePk number,
                  teamId number,
                  timeOnIce text,
                  assists number,
                  goals number,
                  shots number,
                  hits number,
                  powerPlayGoals number,
                  powerPlayAssists number,
                  penaltyMinutes number,
                  faceOffWins number,
                  faceoffTaken number,
                  takeaways number,
                  giveaways number,
                  shortHandedGoals number,
                  shortHandedAssists number,
                  blocked number,
                  plusMinus number,
                  evenTimeOnIce text,
                  powerPlayTimeOnIce text,
                  shortHandedTimeOnIce text,
                  primary key(gamePk, teamId, playerId),
                  foreign key(teamId) references teams(id))''')

    # Create goalie stats table
    # Based on NHL API: https://statsapi.web.nhl.com/api/v1/game/2015020051/feed/live, under liveData -> boxScore -> teams -> away/home -> players -> IDXXXXXX -> stats -> goaliestats
    db.query('''CREATE TABLE player_games_goalie_stats
                 (playerId number,
                  gamePk number,
                  teamId number,
                  timeOnIce text,
                  assists number,
                  goals number,
                  pim number,
                  shots number,
                  saves number,
                  powerPlaySaves number,
                  shortHandedSaves number,
                  evenSaves number,
                  shortHandedShotsAgainst number,
                  evenShotsAgainst number,
                  powerPlayShotsAgainst number,
                  decision text,
                  primary key(gamePk, teamId, playerId),
                  foreign key(teamId) references teams(id))''')

    # Fantasy points, based on Draftkings point values
    db.query('''CREATE TABLE games_draftkings_points
                 (gamePk number,
                  playerId number,
                  points number,
                  primary key (gamePk, playerId),
                  foreign key (gamePk) references games(gamePk),
                  foreign key (playerId) references players(id))''')

    # Picked lineups and results
    create_daily_draftkings_lineups(db)
    create_daily_draftkings_entries(db)

def create_teams(db):

    # Create team data
    url = 'https://statsapi.web.nhl.com/api/v1/teams'
    response = urllib.request.urlopen(url).read()
    data = json.loads(response.decode())
    for team in data['teams']:
        try:
            db.query(
                '''INSERT or REPLACE INTO teams(id, name, link, abbreviation, teamName, locationName, firstYearOfPlay, officialSiteUrl, divisionId, conferenceId, franchiseId, shortName, active) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                [team['id'], team['name'], team['link'], team['abbreviation'], team['teamName'], team['locationName'],
                 team['firstYearOfPlay'], team['officialSiteUrl'], team['division']['id'], team['conference']['id'],
                 team['franchiseId'], team['shortName'], team['active']])
        except Exception as e:
            logging.error("Could not insert the following team:")
            logging.error(team)
            logging.error("Got the following error:")
            logging.error(e)
            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e

def update_games(db, start_date, end_date):

    # Update games data
    url = 'https://statsapi.web.nhl.com/api/v1/schedule?startDate=' + start_date.strftime(
        "%Y-%m-%d") + '&endDate=' + end_date.strftime("%Y-%m-%d")
    response = urllib.request.urlopen(url).read()
    data = json.loads(response.decode())
    for date in data['dates']:
        for game in date['games']:
            # Only want regular season and playoff games (not all-star (A))
            if game['gameType'] in ['R', 'P']:
                logging.info("Updating game ID: " + str(game['gamePk']))
                try:
                    db.query(
                        '''INSERT or REPLACE INTO games(gamePk, link, gameType, season, gameDate, statusCode, awayTeamId, awayScore, homeTeamId, homeScore) VALUES(?,?,?,?,?,?,?,?,?,?)''',
                        [game['gamePk'], game['link'], game['gameType'], game['season'], game['gameDate'],
                         game['status']['statusCode'], game['teams']['away']['team']['id'],
                         game['teams']['away']['score'], game['teams']['home']['team']['id'],
                         game['teams']['home']['score']])
                except Exception as e:
                    logging.error("Could not insert the following game:")
                    logging.error(game)
                    logging.error("Got the following error:")
                    logging.error(e)
                    # Roll back any change if something goes wrong
                    # db.rollback()
                    # raise e


def update_player(db, playerId, force_update = False):

    # TODO: Check into any potential player updates
    db.query("SELECT EXISTS(SELECT 1 FROM players WHERE id = ?) playerExists", (playerId,))
    if db.fetchone()['playerExists'] == 1 and force_update != True:
        logging.info("Skipping player ID: " + str(playerId))
    else:
        try:
            logging.info("Updating player ID: " + str(playerId))
            url = 'https://statsapi.web.nhl.com/api/v1/people/' + str(playerId)
            response = urllib.request.urlopen(url).read()
            data = json.loads(response.decode())

            for player in data['people']:

                try:
                    if "currentTeam" in player:
                        currentTeamId = player['currentTeam']['id']
                    else:
                        currentTeamId = None

                    playerList = [player['id'],
                                  player['fullName'],
                                  player['link'],
                                  player['firstName'],
                                  player['lastName'],
                                  # player['primaryNumber'],
                                  player['birthDate'],
                                  # player['currentAge'],
                                  player['birthCity'],
                                  # player['birthStateProvince'],
                                  player['birthCountry'],
                                  player['height'],
                                  player['weight'],
                                  player['active'],
                                  # player['alternateCaptain'],
                                  # player['captain'],
                                  player['rookie'],
                                  player['shootsCatches'],
                                  player['rosterStatus'],
                                  currentTeamId,
                                  player['primaryPosition']['abbreviation']]

                    db.query('''INSERT OR REPLACE INTO players
                            (id,
                            fullName,
                            link,
                            firstName,
                            lastName,
                            birthDate,
                            birthCity,
                            birthCountry,
                            height,
                            weight,
                            active,
                            rookie,
                            shootsCatches,
                            rosterStatus,
                            currentTeamId,
                            primaryPositionAbbr) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', playerList)
                except Exception as e:
                    logging.error("Could not insert the following player stats:")
                    logging.error(player)
                    logging.error("Got the following error:")
                    logging.error(e)
                    # Roll back any change if something goes wrong
                    # db.rollback()
                    # raise e

        except Exception as e:
            logging.error("Could not connect to player API:")
            logging.error("Got the following error:")
            logging.error(e)
            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e


def update_players(db, update_date):

    # Create player stats data
    # Loop through all games in DB
    db.query('''SELECT DISTINCT gpss.playerId
                FROM games g
                LEFT JOIN player_games_skater_stats gpss ON g.gamePk = gpss.gamePk
                WHERE g.gameDate > ?

                UNION ALL

                SELECT DISTINCT gpgs.playerId
                FROM games g
                LEFT JOIN player_games_goalie_stats gpgs ON g.gamePk = gpgs.gamePk
                WHERE g.gameDate > ?''', (update_date, update_date))
    for players in db.fetchall():
        playerId = players['playerId']
        update_player(db, playerId)


def update_games_players_skaters_stats(db, update_date):

    # Create player stats data
    # Loop through all games in DB
    db.query('''SELECT gamePk, awayTeamId, homeTeamId FROM games WHERE gameDate > ?''', (update_date,))
    for game in db.fetchall():
        gamePk = game['gamePk']
        url = 'https://statsapi.web.nhl.com/api/v1/game/' + str(gamePk) + '/feed/live'
        response = urllib.request.urlopen(url).read()
        data = json.loads(response.decode())

        if data['gameData']['status']['statusCode'] != "7":  # Game isn't final, skip
            logging.info("Game not finished, skipping player stats for game ID: " + str(gamePk))
        else:
            logging.info("Updating player stats for game ID: " + str(gamePk))
            awayTeamId = data['liveData']['boxscore']['teams']['away']['team']['id']
            homeTeamId = data['liveData']['boxscore']['teams']['home']['team']['id']

            for playerIndex in data['liveData']['boxscore']['teams']['away']['players']:
                player = data['liveData']['boxscore']['teams']['away']['players'][playerIndex]
                position = player['position']['abbreviation']
                # Only want skaters, not goalies
                if position in ['RW', 'LW', 'C', 'D']:
                    try:
                        statsList = [gamePk, awayTeamId, player['person']['id'],
                                     player['stats']['skaterStats']['timeOnIce'],
                                     player['stats']['skaterStats']['assists'],
                                     player['stats']['skaterStats']['goals'],
                                     player['stats']['skaterStats']['shots'],
                                     player['stats']['skaterStats']['hits'],
                                     player['stats']['skaterStats']['powerPlayGoals'],
                                     player['stats']['skaterStats']['powerPlayAssists'],
                                     player['stats']['skaterStats']['penaltyMinutes'],
                                     player['stats']['skaterStats']['faceOffWins'],
                                     player['stats']['skaterStats']['faceoffTaken'],
                                     player['stats']['skaterStats']['takeaways'],
                                     player['stats']['skaterStats']['giveaways'],
                                     player['stats']['skaterStats']['shortHandedGoals'],
                                     player['stats']['skaterStats']['shortHandedAssists'],
                                     player['stats']['skaterStats']['blocked'],
                                     player['stats']['skaterStats']['plusMinus'],
                                     player['stats']['skaterStats']['evenTimeOnIce'],
                                     player['stats']['skaterStats']['powerPlayTimeOnIce'],
                                     player['stats']['skaterStats']['shortHandedTimeOnIce']]

                        db.query('''INSERT or REPLACE INTO player_games_skater_stats
                                 (gamePk,
                                  teamId,
                                  playerId,
                                  timeOnIce,
                                  assists,
                                  goals,
                                  shots,
                                  hits,
                                  powerPlayGoals,
                                  powerPlayAssists,
                                  penaltyMinutes,
                                  faceOffWins,
                                  faceoffTaken,
                                  takeaways,
                                  giveaways,
                                  shortHandedGoals,
                                  shortHandedAssists,
                                  blocked,
                                  plusMinus,
                                  evenTimeOnIce,
                                  powerPlayTimeOnIce,
                                  shortHandedTimeOnIce) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                                  statsList)
                    except Exception as e:
                        logging.error("Could not insert the following player stats:")
                        logging.error(player)
                        logging.error("Got the following error:")
                        logging.error(e)
                        # Roll back any change if something goes wrong
                        # db.rollback()
                        # raise e

            for playerIndex in data['liveData']['boxscore']['teams']['home']['players']:
                player = data['liveData']['boxscore']['teams']['home']['players'][playerIndex]
                position = player['position']['abbreviation']
                # Only want skaters, not goalies
                if position in ['RW', 'LW', 'C', 'D']:
                    try:
                        statsList = [gamePk, homeTeamId, player['person']['id'],
                                     player['stats']['skaterStats']['timeOnIce'],
                                     player['stats']['skaterStats']['assists'],
                                     player['stats']['skaterStats']['goals'],
                                     player['stats']['skaterStats']['shots'],
                                     player['stats']['skaterStats']['hits'],
                                     player['stats']['skaterStats']['powerPlayGoals'],
                                     player['stats']['skaterStats']['powerPlayAssists'],
                                     player['stats']['skaterStats']['penaltyMinutes'],
                                     player['stats']['skaterStats']['faceOffWins'],
                                     player['stats']['skaterStats']['faceoffTaken'],
                                     player['stats']['skaterStats']['takeaways'],
                                     player['stats']['skaterStats']['giveaways'],
                                     player['stats']['skaterStats']['shortHandedGoals'],
                                     player['stats']['skaterStats']['shortHandedAssists'],
                                     player['stats']['skaterStats']['blocked'],
                                     player['stats']['skaterStats']['plusMinus'],
                                     player['stats']['skaterStats']['evenTimeOnIce'],
                                     player['stats']['skaterStats']['powerPlayTimeOnIce'],
                                     player['stats']['skaterStats']['shortHandedTimeOnIce']]

                        db.query('''INSERT or REPLACE INTO player_games_skater_stats
                                 (gamePk,
                                  teamId,
                                  playerId,
                                  timeOnIce,
                                  assists,
                                  goals,
                                  shots,
                                  hits,
                                  powerPlayGoals,
                                  powerPlayAssists,
                                  penaltyMinutes,
                                  faceOffWins,
                                  faceoffTaken,
                                  takeaways,
                                  giveaways,
                                  shortHandedGoals,
                                  shortHandedAssists,
                                  blocked,
                                  plusMinus,
                                  evenTimeOnIce,
                                  powerPlayTimeOnIce,
                                  shortHandedTimeOnIce) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                                  statsList)
                    except Exception as e:
                        logging.error("Could not insert the following player stats:")
                        logging.error(player)
                        logging.error("Got the following error:")
                        logging.error(e)
                        # Roll back any change if something goes wrong
                        # db.rollback()
                        # raise e


def update_player_games_goalie_stats(db, update_date):
    # TODO: Potentially switch to game log data and season stats by person?
    #   https://statsapi.web.nhl.com/api/v1/people/8471679?expand=person.stats&stats=gameLog,careerRegularSeason&expand=stats.team&site=en_nhlCA
    # Update goalie stats data
    # Loop through all games in DB
    db.query('''SELECT gamePk, awayTeamId, homeTeamId FROM games WHERE gameDate > ?''', (update_date,))
    for game in db.fetchall():
        gamePk = game['gamePk']
        url = 'https://statsapi.web.nhl.com/api/v1/game/' + str(gamePk) + '/feed/live'
        response = urllib.request.urlopen(url).read()
        data = json.loads(response.decode())
        if data['gameData']['status']['statusCode'] != "7":  # Game isn't final, skip
            logging.info("Game not finished, skipping goalie stats for game ID: " + str(gamePk))
        else:
            logging.info("Updating goalie stats for game ID: " + str(gamePk))
            awayTeamId = data['liveData']['boxscore']['teams']['away']['team']['id']
            homeTeamId = data['liveData']['boxscore']['teams']['home']['team']['id']

            for playerIndex in data['liveData']['boxscore']['teams']['away']['players']:
                player = data['liveData']['boxscore']['teams']['away']['players'][playerIndex]
                position = player['position']['abbreviation']
                # Only want goalies, not skaters
                if position == 'G':
                    try:
                        statsList = [gamePk, awayTeamId, player['person']['id'],
                                     player['stats']['goalieStats']['timeOnIce'],
                                     player['stats']['goalieStats']['assists'],
                                     player['stats']['goalieStats']['goals'],
                                     player['stats']['goalieStats']['pim'],
                                     player['stats']['goalieStats']['shots'],
                                     player['stats']['goalieStats']['saves'],
                                     player['stats']['goalieStats']['powerPlaySaves'],
                                     player['stats']['goalieStats']['shortHandedSaves'],
                                     player['stats']['goalieStats']['evenSaves'],
                                     player['stats']['goalieStats']['shortHandedShotsAgainst'],
                                     player['stats']['goalieStats']['evenShotsAgainst'],
                                     player['stats']['goalieStats']['powerPlayShotsAgainst'],
                                     player['stats']['goalieStats']['decision']]
                        db.query('''INSERT or REPLACE INTO player_games_goalie_stats
                                 (gamePk,
                                  teamId,
                                  playerId,
                                  timeOnIce,
                                  assists,
                                  goals,
                                  pim,
                                  shots,
                                  saves,
                                  powerPlaySaves,
                                  shortHandedSaves,
                                  evenSaves,
                                  shortHandedShotsAgainst,
                                  evenShotsAgainst,
                                  powerPlayShotsAgainst,
                                  decision) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', statsList)
                    except Exception as e:
                        logging.error("Could not insert the following player stats:")
                        logging.error(player)
                        logging.error("Got the following error:")
                        logging.error(e)
                        # Roll back any change if something goes wrong
                        # db.rollback()
                        # raise e

            for playerIndex in data['liveData']['boxscore']['teams']['home']['players']:
                player = data['liveData']['boxscore']['teams']['home']['players'][playerIndex]
                position = player['position']['abbreviation']
                # Only want goalies, not skaters
                if position == 'G':
                    try:
                        statsList = [gamePk, homeTeamId, player['person']['id'],
                                     player['stats']['goalieStats']['timeOnIce'],
                                     player['stats']['goalieStats']['assists'],
                                     player['stats']['goalieStats']['goals'],
                                     player['stats']['goalieStats']['pim'],
                                     player['stats']['goalieStats']['shots'],
                                     player['stats']['goalieStats']['saves'],
                                     player['stats']['goalieStats']['powerPlaySaves'],
                                     player['stats']['goalieStats']['shortHandedSaves'],
                                     player['stats']['goalieStats']['evenSaves'],
                                     player['stats']['goalieStats']['shortHandedShotsAgainst'],
                                     player['stats']['goalieStats']['evenShotsAgainst'],
                                     player['stats']['goalieStats']['powerPlayShotsAgainst'],
                                     player['stats']['goalieStats']['decision']]

                        db.query('''INSERT or REPLACE INTO player_games_goalie_stats
                                 (gamePk,
                                  teamId,
                                  playerId,
                                  timeOnIce,
                                  assists,
                                  goals,
                                  pim,
                                  shots,
                                  saves,
                                  powerPlaySaves,
                                  shortHandedSaves,
                                  evenSaves,
                                  shortHandedShotsAgainst,
                                  evenShotsAgainst,
                                  powerPlayShotsAgainst,
                                  decision) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', statsList)
                    except Exception as e:
                        logging.error("Could not insert the following player stats:")
                        logging.error(player)
                        logging.error("Got the following error:")
                        logging.error(e)
                        # Roll back any change if something goes wrong
                        # db.rollback()
                        # raise e


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
                LEFT JOIN player_games_skater_stats gpss ON g.gamePk = gpss.gamePk
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
                LEFT JOIN player_games_goalie_stats gpgs ON g.gamePk = gpgs.gamePk
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

def update_game_info(db, update_type = "week_ago"):
    # Update for previous week, will overwrite any data
    day_ago = datetime.date.today() - datetime.timedelta(days=1)
    three_days_ago = datetime.date.today() - datetime.timedelta(days=3)
    week_ago = datetime.date.today() - datetime.timedelta(days=7)
    start_of_season = datetime.date(2016, 10, 12)
    last_season = datetime.date(2015, 10, 1)

    if update_type == "day_ago":
        update_as_of = day_ago
    elif update_type == "three_days_ago":
        update_as_of = three_days_ago
    elif update_type == "start_of_season":
        update_as_of = start_of_season
    elif update_type == "last_season":
        update_as_of = last_season
    else:
        update_as_of = week_ago

    update_games(db, update_as_of, datetime.date.today() + datetime.timedelta(days=1))
    update_games_players_skaters_stats(db, update_as_of)
    update_player_games_goalie_stats(db, update_as_of)
    update_players(db, update_as_of)

    # Find point values
    update_games_draftkings_points(db, update_as_of)

    db.commit()