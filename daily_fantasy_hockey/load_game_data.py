import json
import sqlite3
import datetime
import urllib.request


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def connect(sqlite_file):
    """ Make connection to an SQLite database file """
    conn = sqlite3.connect(sqlite_file)

    # Allow rows to be reference by column name
    conn.row_factory = dict_factory

    c = conn.cursor()
    return conn, c


def close(conn):
    """ Commit changes and close connection to the database """
    # conn.commit()
    conn.close()

def total_rows(cursor, table_name, print_out=False):
    """ Returns the total number of rows in the database """
    c.execute('SELECT COUNT(*) FROM {}'.format(table_name))
    count = c.fetchall()
    if print_out:
        print('\nTotal rows: {}'.format(count[0][0]))
    return count[0][0]


def table_col_info(cursor, table_name, print_out=False):
    """ Returns a list of tuples with column informations:
        (id, name, type, notnull, default_value, primary_key)
    """
    c.execute('PRAGMA TABLE_INFO({})'.format(table_name))
    info = c.fetchall()

    if print_out:
        print("\nColumn Info:\nID, Name, Type, NotNull, DefaultVal, PrimaryKey")
        for col in info:
            print(col)
    return info


def values_in_col(cursor, table_name, print_out=True):
    """ Returns a dictionary with columns as keys and the number of not-null
        entries as associated values.
    """
    c.execute('PRAGMA TABLE_INFO({})'.format(table_name))
    info = c.fetchall()
    col_dict = dict()
    for col in info:
        col_dict[col[1]] = 0
    for col in col_dict:
        c.execute('SELECT ({0}) FROM {1} WHERE {0} IS NOT NULL'.format(col, table_name))
        # In my case this approach resulted in a better performance than using COUNT
        number_rows = len(c.fetchall())
        col_dict[col] = number_rows
    if print_out:
        print("\nNumber of entries per column:")
        for i in col_dict.items():
            print('{}: {}'.format(i[0], i[1]))
    return col_dict


def drop_tables(c):
    try:
        c.execute('''DROP table games_draftkings_points''')
    except Exception as e:
        print("Could not drop tables, got the following error:")
        print(e)

    try:
        c.execute('''DROP table players''')
    except Exception as e:
        print("Could not drop tables, got the following error:")
        print(e)

    try:
        c.execute('''DROP table games_players_goalie_stats''')
    except Exception as e:
        print("Could not drop tables, got the following error:")
        print(e)

    try:
        c.execute('''DROP table games_players_skater_stats''')
    except Exception as e:
        print("Could not drop tables, got the following error:")
        print(e)

    try:
        c.execute('''DROP table games''')
    except Exception as e:
        print("Could not drop tables, got the following error:")
        print(e)

    try:
        c.execute('''DROP table teams''')
    except Exception as e:
        print("Could not drop tables, got the following error:")
        print(e)


def create_tables(c):
    # Create teams table
    # Based on NHL API: https://statsapi.web.nhl.com/api/v1/teams
    c.execute('''CREATE TABLE teams
                 (id number primary key, name text, link text, abbreviation text, teamName text, locationName text, firstYearOfPlay text, officialSiteUrl text, divisionId number, conferenceId number, franchiseId number, shortName text, active boolean)''')

    # Create games table
    # Based on NHL API: https://statsapi.web.nhl.com/api/v1/schedule?startDate=2015-10-06&endDate=2016-05-08
    c.execute('''CREATE TABLE games
                 (gamePk number primary key, link text, gameType text, season number, gameDate date, statusCode number, awayTeamId number, awayScore number, homeTeamId number, homeScore number,
                 foreign key(awayTeamId) references teams(id),
                 foreign key(homeTeamId) references teams(id))''')

    # Create player table
    # Based on NHL API: http://statsapi.web.nhl.com/api/v1/people/8474688
    c.execute('''CREATE TABLE players
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


    # Create player stats table
    # Based on NHL API: https://statsapi.web.nhl.com/api/v1/game/2015020051/feed/live, under liveData -> boxScore -> teams -> away/home -> players -> IDXXXXXX -> stats -> skaterStats
    c.execute('''CREATE TABLE games_players_skater_stats
                 (gamePk number,
                  teamId number,
                  playerId number,
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
    c.execute('''CREATE TABLE games_players_goalie_stats
                 (gamePk number,
                  teamId number,
                  playerId number,
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
    c.execute('''CREATE TABLE games_draftkings_points
                 (gamePk number,
                 playerId number,
                 points number,
                 primary key (gamePk, playerId),
                 foreign key (gamePk) references games(gamePk),
                 foreign key (playerId) references players(id))''')



def create_teams(c):
    # Create team data
    url = 'https://statsapi.web.nhl.com/api/v1/teams'
    response = urllib.request.urlopen(url).read()
    data = json.loads(response.decode())
    for team in data['teams']:
        try:
            c.execute(
                '''INSERT or REPLACE INTO teams(id, name, link, abbreviation, teamName, locationName, firstYearOfPlay, officialSiteUrl, divisionId, conferenceId, franchiseId, shortName, active) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                [team['id'], team['name'], team['link'], team['abbreviation'], team['teamName'], team['locationName'],
                 team['firstYearOfPlay'], team['officialSiteUrl'], team['division']['id'], team['conference']['id'],
                 team['franchiseId'], team['shortName'], team['active']])
        except Exception as e:
            print("Could not insert the following team:")
            print(team)
            print("Got the following error:")
            print(e)
            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e

def update_games(c, start_date, end_date):
    # Update games data
    url = 'https://statsapi.web.nhl.com/api/v1/schedule?startDate=' + start_date.strftime(
        "%Y-%m-%d") + '&endDate=' + end_date.strftime("%Y-%m-%d")
    response = urllib.request.urlopen(url).read()
    data = json.loads(response.decode())
    for date in data['dates']:
        for game in date['games']:
            # Only want regular season and playoff games (not all-star (A))
            if game['gameType'] in ['R', 'P']:
                print("Updating game ID: " + str(game['gamePk']))
                try:
                    c.execute(
                        '''INSERT or REPLACE INTO games(gamePk, link, gameType, season, gameDate, statusCode, awayTeamId, awayScore, homeTeamId, homeScore) VALUES(?,?,?,?,?,?,?,?,?,?)''',
                        [game['gamePk'], game['link'], game['gameType'], game['season'], game['gameDate'],
                         game['status']['statusCode'], game['teams']['away']['team']['id'],
                         game['teams']['away']['score'], game['teams']['home']['team']['id'],
                         game['teams']['home']['score']])
                except Exception as e:
                    print("Could not insert the following game:")
                    print(game)
                    print("Got the following error:")
                    print(e)
                    # Roll back any change if something goes wrong
                    # db.rollback()
                    # raise e

def update_player(playerId):
    # TODO: Check into any potential player updates
    c.execute("SELECT EXISTS(SELECT 1 FROM players WHERE id = ?) playerExists", (playerId,))
    if c.fetchone()['playerExists'] == 1:
        print("Skipping player ID: " + str(playerId))
    else:
        try:
            print("Updating player ID: " + str(playerId))
            url = 'https://statsapi.web.nhl.com/api/v1/people/' + str(playerId)
            response = urllib.request.urlopen(url).read()
            data = json.loads(response.decode())

            for player in data['people']:

                # if "currentTeam" in player:
                #     currentTeam = player['currentTeam']['id']
                # else:
                #     currentTeam = ""

                try:
                    playerList = [player['id'],
                                  player['fullName'],
                                  player['link'],
                                  player['firstName'],
                                  player['lastName'],
                                  #player['primaryNumber'],
                                  player['birthDate'],
                                  #player['currentAge'],
                                  player['birthCity'],
                                  #player['birthStateProvince'],
                                  player['birthCountry'],
                                  player['height'],
                                  player['weight'],
                                  player['active'],
                                  #player['alternateCaptain'],
                                  #player['captain'],
                                  player['rookie'],
                                  player['shootsCatches'],
                                  player['rosterStatus'],
                                  player['currentTeam']['id'],
                                  player['primaryPosition']['abbreviation']]

                    c.execute('''INSERT OR IGNORE INTO players
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
                    print("Could not insert the following player stats:")
                    print(player)
                    print("Got the following error:")
                    print(e)
                    # Roll back any change if something goes wrong
                    # db.rollback()
                    # raise e

        except Exception as e:
            print("Could not connect to player API:")
            print("Got the following error:")
            print(e)
            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e

def update_players(c, update_date):
    # Create player stats data
    # Loop through all games in DB
    c.execute('''SELECT DISTINCT gpss.playerId
                FROM games g
                LEFT JOIN games_players_skater_stats gpss ON g.gamePk = gpss.gamePk
                WHERE g.gameDate > ?

                UNION ALL

                SELECT DISTINCT gpgs.playerId
                FROM games g
                LEFT JOIN games_players_goalie_stats gpgs ON g.gamePk = gpgs.gamePk
                WHERE g.gameDate > ?''', (update_date,update_date))
    for players in c.fetchall():
        playerId = players['playerId']
        update_player(playerId)

def update_games_players_skaters_stats(c, update_date):
    # Create player stats data
    # Loop through all games in DB
    c.execute('''SELECT gamePk, awayTeamId, homeTeamId FROM games WHERE gameDate > ?''', (update_date,))
    for game in c.fetchall():
        gamePk = game['gamePk']
        url = 'https://statsapi.web.nhl.com/api/v1/game/' + str(gamePk) + '/feed/live'
        response = urllib.request.urlopen(url).read()
        data = json.loads(response.decode())

        if data['gameData']['status']['statusCode'] != "7": # Game isn't final, skip
            print("Game not finished, skipping player stats for game ID: " + str(gamePk))
        else:
            print("Updating player stats for game ID: " + str(gamePk))
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

                        c.execute('''INSERT or REPLACE INTO games_players_skater_stats
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
                                  shortHandedTimeOnIce) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', statsList)
                    except Exception as e:
                        print("Could not insert the following player stats:")
                        print(player)
                        print("Got the following error:")
                        print(e)
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

                        c.execute('''INSERT or REPLACE INTO games_players_skater_stats
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
                                  shortHandedTimeOnIce) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', statsList)
                    except Exception as e:
                        print("Could not insert the following player stats:")
                        print(player)
                        print("Got the following error:")
                        print(e)
                        # Roll back any change if something goes wrong
                        # db.rollback()
                        # raise e

def update_games_players_goalie_stats(c, update_date):
    # Update goalie stats data
    # Loop through all games in DB
    c.execute('''SELECT gamePk, awayTeamId, homeTeamId FROM games WHERE gameDate > ?''', (update_date,))
    for game in c.fetchall():
        gamePk = game['gamePk']
        url = 'https://statsapi.web.nhl.com/api/v1/game/' + str(gamePk) + '/feed/live'
        response = urllib.request.urlopen(url).read()
        data = json.loads(response.decode())
        if data['gameData']['status']['statusCode'] != "7": # Game isn't final, skip
            print("Game not finished, skipping goalie stats for game ID: " + str(gamePk))
        else:
            print("Updating goalie stats for game ID: " + str(gamePk))
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
                        c.execute('''INSERT or REPLACE INTO games_players_goalie_stats
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
                        print("Could not insert the following player stats:")
                        print(player)
                        print("Got the following error:")
                        print(e)
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

                        c.execute('''INSERT or REPLACE INTO games_players_goalie_stats
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
                        print("Could not insert the following player stats:")
                        print(player)
                        print("Got the following error:")
                        print(e)
                        # Roll back any change if something goes wrong
                        # db.rollback()
                        # raise e

def update_games_draftkings_points(c, update_date):

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
    c.execute('''SELECT gpss.*
                FROM games g
                LEFT JOIN games_players_skater_stats gpss ON g.gamePk = gpss.gamePk
                WHERE g.gameDate > ?''', (update_date,))
    for player_stats in c.fetchall():
        try:
            gamePk = player_stats['gamePk']
            playerId = player_stats['playerId']
            points = player_stats["goals"] * 3 + player_stats["assists"] * 2 + player_stats["shots"] * 0.5 + player_stats["blocked"] * 0.5 + player_stats["shortHandedGoals"] + player_stats["shortHandedAssists"] #+ player_stats["Shootout"] * 0.2
            if player_stats["goals"] >= 3:
                points += 1.5

            c.execute('''INSERT or REPLACE INTO games_draftkings_points
                     (gamePk,
                      playerId,
                      points) VALUES(?,?,?)''', (gamePk,playerId,points))

        except Exception as e:
            print("Could not insert the following player points for DraftKings:")
            print(player_stats)
            print("Got the following error:")
            print(e)
            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e

    # Goalie stats
    c.execute('''SELECT gpgs.*
                FROM games g
                LEFT JOIN games_players_goalie_stats gpgs ON g.gamePk = gpgs.gamePk
                WHERE g.gameDate > ?''', (update_date,))
    for goalie_stats in c.fetchall():
        try:
            gamePk = goalie_stats['gamePk']
            playerId = goalie_stats['playerId']

            goals_against = (goalie_stats["shots"] - goalie_stats["saves"])
            points = goalie_stats["saves"] * 0.3 - goals_against + goalie_stats["goals"] * 3 + goalie_stats["assists"] * 2
            if goalie_stats["decision"] == "W":
                points += 3
            if goals_against == 0:
                # check time on ice as well, must be at least 60 minutes (3600 s)
                min, seconds = [int(i) for i in goalie_stats["timeOnIce"].split(':')]
                if (min * 60 + seconds) > 3600:
                    points += 2


            c.execute('''INSERT or REPLACE INTO games_draftkings_points
                     (gamePk,
                      playerId,
                      points) VALUES(?,?,?)''', (gamePk,playerId,points))

        except Exception as e:
            print("Could not insert the following goalie points for DraftKings:")
            print(goalie_stats)
            print("Got the following error:")
            print(e)
            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e

    # if position == "G":
    #     row = get_goalie_stats(name, date)
    #     fpp_night = row["W"] * 3 + row["SV"] * 0.3 - row["GA"] * 1 + row["SO"] * 2
    #     return fpp_night
    # else:
    #     row = get_player_stats(name, date)
    #     fpp_night = row["G"] * 3 + row["A"] * 2 + row["SOG"] * 0.5 + row["B"] * 0.5 + row["SHP"] * 1 + row["Shootout"] * 0.2\
    #     if row["G"] >= 3:
    #         fpp_night += 1.5
    #     return fpp_night

if __name__ == '__main__':
    sqlite_file = 'daily_fantasy_hockey_db.sqlite'

    conn, c = connect(sqlite_file)
    # Need to turn on foreign keys, not on by default
    c.execute('''PRAGMA foreign_keys = ON''')

    # Drop and create tables, start from scratch
    # drop_tables(c)
    # create_tables(c)
    # create_teams(c)

    # Update for previous week, will overwrite any data
    week_ago = datetime.date.today() - datetime.timedelta(days=7)
    start_of_season = datetime.date(2016,10,12)

    update_games(c, week_ago, datetime.date.today())
    conn.commit()

    update_games_players_skaters_stats(c, week_ago)
    conn.commit()

    update_games_players_goalie_stats(c, week_ago)
    conn.commit()

    update_players(c, week_ago)
    conn.commit()

    # Find point values
    update_games_draftkings_points(c, week_ago)
    conn.commit()

    close(conn)