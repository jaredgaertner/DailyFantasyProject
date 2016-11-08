import csv
import sqlite3
import time
import datetime
import logging
import random

__author__ = "jaredg"

# Configure logging
logFormatter = logging.Formatter(
    "%(asctime)s [%(threadName)s][%(levelname)s][%(filename)s:%(lineno)s - %(funcName)s()] %(message)s")
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler("../resources/logs/lineup_builder.log." + time.strftime("%Y%m%d-%H%M%S"))
fileHandler.setFormatter(logFormatter)
# fileHandler.setLevel(logging.DEBUG)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
# consoleHandler.setLevel(logging.INFO)
rootLogger.addHandler(consoleHandler)
rootLogger.setLevel(logging.INFO)


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


# Find up to max_size pairs of players with maximum value to weight ratio
def find_player_pair(players, position, max_set_size=200):
    players = [item for item in players if item['position'] == position]
    logging.debug("Number of " + position + " being used in pairs: " + str(len(players)))
    set_of_players = []
    for i in range(0, len(players) - 1):
        for j in range(i + 1, len(players) - 1):
            player_pair = {"nameAndId": [players[i]['nameAndId'], players[j]['nameAndId']],
                           "weight": players[i]['weight'] + players[j]['weight'],
                           "value": players[i]['value'] + players[j]['value'],
                           "position": position}
            set_of_players.append(player_pair)

    # Take the max_size number of optimal value to weight ratio and the max_size number of highest value pairs of the rest left
    logging.debug("Total number of " + position + " pairs: " + str(len(set_of_players)))
    return set_of_players
    # sorted_set_of_players_optimal = sorted(set_of_players, key=lambda tup: tup['value'] / tup['weight'], reverse=True)
    # sorted_set_of_players_highest_value = sorted(sorted_set_of_players_optimal[max_set_size:], key=lambda tup: tup['value'],
    #                                              reverse=True)
    # return sorted_set_of_players_optimal[:max_set_size] + sorted_set_of_players_highest_value[:max_set_size]


def find_player_triples(players, position, max_triple_set_size=20000):
    players = [item for item in players if item['position'] == position]
    logging.debug("Number of " + position + " being used in triples: " + str(len(players)))
    set_of_players = []
    for i in range(0, len(players) - 1):
        for j in range(i + 1, len(players) - 1):
            for k in range(i + j + 1, len(players) - 1):
                player_triple = {
                    "nameAndId": [players[i]['nameAndId'], players[j]['nameAndId'], players[k]['nameAndId']],
                    "weight": players[i]['weight'] + players[j]['weight'] + players[k]['weight'],
                    "value": players[i]['value'] + players[j]['value'] + players[k]['value'], "position": position}
                set_of_players.append(player_triple)

    # Take the max_size number of optimal value to weight ratio and the max_size number of highest value triplets
    logging.debug("Total number of " + position + " triples: " + str(len(set_of_players)))
    return set_of_players
    # sorted_set_of_players_optimal = sorted(set_of_players, key=lambda tup: tup['value'] / tup['weight'], reverse=True)
    # sorted_set_of_players_highest_value = sorted(sorted_set_of_players_optimal[max_triple_set_size:],
    #                                              key=lambda tup: tup['value'], reverse=True)
    # return sorted_set_of_players_optimal[:max_triple_set_size] + sorted_set_of_players_highest_value[
    #                                                              :max_triple_set_size]


def get_player_value(playerId):
    logging.debug("Getting player value for player ID:" + str(playerId))

    # Find average points for last week and for the year
    c.execute('''select p.id,
                        AVG(case when g.gamePk like '2016%' then gdp.points else null end) AS average_points_for_year,
                        ifnull(AVG(case when g.gameDate > date('now','-7 day') then gdp.points else null end),0) AS average_points_for_week
                 from games_draftkings_points gdp
                 inner join players p
                 on gdp.playerid = p.id
                 inner join games g
                 on gdp.gamePk = g.gamePk
                 where (g.gamePk like '2016%' or
                       g.gameDate > date('now','-7 day')) and
                       p.id = ?
                 group by p.id''', (playerId,))
    value = 0
    for player_stats in c.fetchall():
        # Calculate value
        value = 0.75 * player_stats['average_points_for_year'] + 0.25 * player_stats['average_points_for_week']
        logging.debug("Got value of " + str(value))
    return value

            # # Adjust value based on opponent
        # c.execute('''select p.id,
        #                     ts.
        #              from games_draftkings_points gdp
        #              inner join players p
        #              on gdp.playerid = p.id
        #              inner join games g
        #              on gdp.gamePk = g.gamePk
        #              where p.id = ?''', (playerId,))
        #
        # for player_stats in c.fetchall():
        #     # Calculate value
        #     value *= goals_against_percentage
        #     logging.debug("Got value of " + str(goals_against_percentage))

    # except Exception as e:
    #     logging.error("Could not find player points for DraftKings:")
    #     logging.error("Got the following error:")
    #     logging.error(e)
    #     return 0


def get_player_position(position):
    if position in ["LW", "RW"]:
        return "W"
    else:
        return position

# def get_opponent(c, player_id, game_pk):
#     try:
#         c.execute('''select p.id
#                      from players p
#                      where p.fullName = ?''', (name,))
#         for player in c.fetchall():
#             return player['id']
#
#     except Exception as e:
#         logging.error("Could not find player ID for " + name)
#         logging.error("Got the following error:")
#         logging.error(e)
#         return 0

# def get_opponent_by_game_info(game_info, team_abbrev):
#     split_info = game_info.split()
#     logging.info(split_info)
#     teams = split_info[0].split(str="@")
#     logging.info(teams)
#     if teams[0].upper() != team_abbrev.upper():
#         return teams[1].upper()
#     else:
#         return teams[0].upper()

# def get_game_pk(c, player_id, current_date):
#     try:
#         c.execute('''select p.id
#                      from players p
#                      inner join games g
#                      on p.id = g.
#                      where p.id = ?''', (player_id,current_date,))
#         for player in c.fetchall():
#             return player['id']
#
#     except Exception as e:
#         logging.error("Could not find gamePk for " + player_id + " on date " + str(current_date))
#         logging.error("Got the following error:")
#         logging.error(e)
#         return 0

def get_player_id(name):
    try:
        c.execute('''select p.id
                     from players p
                     where p.fullName = ?''', (name,))
        for player in c.fetchall():
            return player['id']

    except Exception as e:
        logging.error("Could not find player ID for " + name)
        logging.error("Got the following error:")
        logging.error(e)
        return 0


def get_player_id_by_name_and_draftkings_id(nameAndId):
    try:
        c.execute('''select pdi.playerId
                     from player_draftkings_info pdi
                     where pdi.nameAndId = ?''', (nameAndId,))
        for player in c.fetchall():
            return player['playerId']

    except Exception as e:
        logging.error("Could not find player ID for " + nameAndId)
        logging.error("Got the following error:")
        logging.error(e)
        return 0


def get_lineup_value(lineup):
    logging.debug("Getting lineup value")
    logging.debug(lineup)
    try:
        total_value = 0
        for playerId in lineup:
            total_value += get_player_value(playerId)
        return total_value

    except Exception as e:
        logging.error("Could not get lineup value")
        logging.error(lineup)
        logging.error("Got the following error:")
        logging.error(e)
        return 0


def insert_player_data(player_info):
    logging.debug("Inserting player information for DraftKings for " + str(player_info['name']))
    try:
        player_info_list = [player_info['id'],
                            player_info['name'],
                            player_info['nameAndId'],
                            player_info['playerId'],
                            player_info['weight'],
                            player_info['value'],
                            player_info['position'],
                            player_info['gameInfo'],
                            player_info['teamAbbrev'],
                            player_info['gameDate'],
                            player_info['draftType']]

        c.execute('''INSERT OR IGNORE INTO player_draftkings_info
                (id,
                 name,
                 nameAndId,
                 playerId,
                 weight,
                 value,
                 position,
                 gameInfo,
                 teamAbbrev,
                 gameDate,
                 draftType) VALUES(?,?,?,?,?,?,?,?,?,?,?)''', player_info_list)

    except Exception as e:
        logging.error("Could not insert player info for DraftKings:")
        logging.error(player_info)
        logging.error("Got the following error:")
        logging.error(e)
        # Roll back any change if something goes wrong
        # db.rollback()
        # raise e


def insert_all_lineups(c, all_lineups, current_date):
    logging.debug("Inserting all player lineups for DraftKings")
    logging.debug(all_lineups)

    try:
        for lineup in all_lineups:
            # Convert lineup to player IDs
            lineupWithPlayerId = []
            lineupWithPlayerId.append(get_player_id_by_name_and_draftkings_id(lineup[0]))
            lineupWithPlayerId.append(get_player_id_by_name_and_draftkings_id(lineup[1]))
            lineupWithPlayerId.append(get_player_id_by_name_and_draftkings_id(lineup[2]))
            lineupWithPlayerId.append(get_player_id_by_name_and_draftkings_id(lineup[3]))
            lineupWithPlayerId.append(get_player_id_by_name_and_draftkings_id(lineup[4]))
            lineupWithPlayerId.append(get_player_id_by_name_and_draftkings_id(lineup[5]))
            lineupWithPlayerId.append(get_player_id_by_name_and_draftkings_id(lineup[6]))
            lineupWithPlayerId.append(get_player_id_by_name_and_draftkings_id(lineup[7]))
            lineupWithPlayerId.append(get_player_id_by_name_and_draftkings_id(lineup[8]))
            total_weight = lineup[9]
            adjusted_total_value = lineup[10]
            logging.debug(lineupWithPlayerId)

            # Get the actual total value, as the value in the lineup is adjusted
            total_value = get_lineup_value(lineupWithPlayerId)
            logging.debug("Initial total value: " + str(total_value))
            logging.debug("Adjusted total value: " + str(adjusted_total_value))

            # Insert the lineup
            all_lineups_list = [current_date,
                                lineupWithPlayerId[0],
                                lineupWithPlayerId[1],
                                lineupWithPlayerId[2],
                                lineupWithPlayerId[3],
                                lineupWithPlayerId[4],
                                lineupWithPlayerId[5],
                                lineupWithPlayerId[6],
                                lineupWithPlayerId[7],
                                lineupWithPlayerId[8],
                                total_weight,
                                total_value]
            logging.debug(all_lineups_list)
            c.execute('''INSERT OR IGNORE INTO daily_draftkings_lineups
                         (gameDate,
                          centre1,
                          centre2,
                          winger1,
                          winger2,
                          winger3,
                          defence1,
                          defence2,
                          goalie,
                          util,
                          totalWeight,
                          totalValue) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''', all_lineups_list)

    except Exception as e:
        logging.error("Could not insert all lineups for DraftKings:")
        logging.error(all_lineups)
        logging.error("Got the following error:")
        logging.error(e)


        # Roll back any change if something goes wrong
        # db.rollback()
        # raise e


def get_player_data(c, current_date):
    players = []

    # Check if data already exists in database
    c.execute("select exists(select 1 from player_draftkings_info where gameDate = ?) playerInfoExists",
              (current_date,))
    if c.fetchone()['playerInfoExists'] == 1:
        logging.info("Player information for DraftKings already exists, grabbing from database.")
        try:
            c.execute('''select pdi.id,
                                pdi.name,
                                pdi.nameAndId,
                                pdi.playerId,
                                pdi.weight,
                                pdi.value,
                                pdi.position,
                                pdi.gameInfo,
                                pdi.opponent,
                                pdi.gamePk,
                                pdi.teamAbbrev,
                                pdi.gameDate,
                                pdi.draftType
                        from player_draftkings_info pdi
                        where pdi.gameDate = ?''', (current_date,))
            for player_info in c.fetchall():
                players.append(player_info)
            return players

        except Exception as e:
            logging.error("Could not find player info for DraftKings:")
            logging.error(player_info)
            logging.error("Got the following error:")
            logging.error(e)
            return 0

    # Doesn't, so check from file
    else:
        logging.info("Player information for DraftKings doesn't exist, grabbing from csv file.")
        with open("../resources/DKSalaries_" + current_date.strftime("%d%b%Y").upper() + ".csv", "r") as csvfile:
            # Skip the first 7 lines, as it contains the format for uploading
            for i in range(7):
                next(csvfile)

            reader = csv.DictReader(csvfile)
            for row in reader:
                name = row[' Name']
                player_id = get_player_id(name)
                value = get_player_value(player_id)
                game_info = row['GameInfo']
                team_abbrev = row['TeamAbbrev ']
                # game_pk = get_game_pk(c, player_id, current_date)
                player_info = {"id": row[' ID'],
                               "name": name,
                               "nameAndId": row['Name + ID'],
                               "playerId": player_id,
                               "weight": int(int(row[' Salary']) / 100),
                               "value": value,
                               "position": get_player_position(row['Position']),
                               "gameInfo": game_info,
                               # "opponent": get_opponent_by_game_info(game_info, team_abbrev),
                               # "gamePk": game_pk,
                               "teamAbbrev": team_abbrev,
                               "gameDate": current_date,
                               "draftType": "Standard"}
                logging.debug(player_info)
                # player_info = [name_id, weight, value, position]
                players.append(player_info)
                insert_player_data(player_info)
                logging.debug(player_info)

            csvfile.close()
            return players

# http://stackoverflow.com/questions/19389931/knapsack-constraint-python
def multi_choice_knapsack(goalie, util, defensemen, centres, wingers, limit):
    # Remove chosen G and W from limit
    limit -= (goalie['weight'] + util['weight'])
    logging.debug("New limit, after removing chosen G is: " + str(limit))

    # Run multiple-choice knapsack on the pairs of D, C, W, and any skaters for the Util position
    positions = ["C", "W", "D"]
    table = [[0 for w in range(limit + 1)] for j in range(len(positions) + 1)]
    player_added = [[0 for w in range(limit + 1)] for j in range(len(positions) + 1)]
    logging.debug("Knapsack: Going through all " + str(len(positions)) + " positions.")
    for i in range(1, len(positions) + 1):
        logging.debug("Multiple Choice Knapsack: Checking position " + str(positions[i - 1]))
        if positions[i - 1] == "W":
            current_player_set = wingers
        elif positions[i - 1] == "D":
            current_player_set = defensemen
        elif positions[i - 1] == "C":
            current_player_set = centres
        else:
            logging.error("Unknown position!")

        for w in range(1, limit + 1):
            max_val_for_position = table[i - 1][w]
            for player in current_player_set:
                # Find the max for all player_set of that position
                weight = player['weight']
                nameAndId = player['nameAndId']
                value = player['value']
                position = player['position']

                if weight <= w and table[i - 1][w - weight] + value > max_val_for_position:
                    max_val_for_position = table[i - 1][w - weight] + value
                    player_added[i][w] = player
                    logging.debug(
                        "Adding player at (" + str(i) + "," + str(w) + "): " + str(nameAndId) + ", wt: " + str(
                            weight) + ", val: " + str(value) + ", position: " + str(
                            position))
            table[i][w] = max_val_for_position

    result = []
    w = limit
    logging.debug(table[2][w])
    total_value = 0
    total_weight = 0
    for i in range(len(positions), 0, -1):
        for j in range(w, 0, -1):
            was_added = table[i][j - 1] != table[i][j]

            if was_added:
                logging.debug(player_added[i][j])
                weight = player_added[i][j]['weight']
                value = player_added[i][j]['value']

                result.append(player_added[i][j])
                total_value += value
                total_weight += weight
                w -= weight
                break

    # Adding players names to a set of players, results added in reverse order (Util, D, W, C)
    logging.debug(result)
    total_weight += goalie['weight'] + util['weight']
    total_value += goalie['value'] + util['value']
    logging.debug(total_value)
    full_set = [result[2]['nameAndId'][0],
                result[2]['nameAndId'][1],
                result[1]['nameAndId'][0],
                result[1]['nameAndId'][1],
                result[1]['nameAndId'][2],
                result[0]['nameAndId'][0],
                result[0]['nameAndId'][1],
                goalie['nameAndId'],
                util['nameAndId'],
                total_weight,
                total_value]
    set_of_players = []
    set_of_players.append(full_set)
    logging.debug(set_of_players)
    return set_of_players


def knapsack(skaters, goalie, util, limit, max_set_size=2000, max_triple_set_size=400000):
    defensemen = find_player_pair(skaters, "D", max_set_size)
    centres = find_player_pair(skaters, "C", max_set_size)
    wingers = find_player_triples(skaters, "W", max_triple_set_size)

    logging.debug("Number of D pairs being checked: " + str(len(defensemen)))
    logging.debug("Number of C pairs being checked: " + str(len(centres)))
    logging.debug("Number of W pairs being checked: " + str(len(wingers)))

    return multi_choice_knapsack(goalie, util, defensemen, centres, wingers, limit)


def brute_force(skaters, goalie, util, limit, max_set_size=2000):
    defensemen = find_player_pair(skaters, "D", max_set_size)
    centres = find_player_pair(skaters, "C", max_set_size)
    wingers = find_player_triples(skaters, "W", max_set_size)

    logging.info("Number of D pairs being checked: " + str(len(defensemen)))
    logging.info("Number of C pairs being checked: " + str(len(centres)))
    logging.info("Number of W pairs being checked: " + str(len(wingers)))

    set_of_players = []
    index = 0

    logging.info(
        "Potential number of combinations: " + str(len(centres) * len(defensemen) * len(wingers)))
    # for i in range(0, len(goalies)-1):
    for j in range(len(centres)):
        for k in range(len(defensemen)):
            for l in range(len(wingers)):
                # for m in range(len(skaters)):
                #     if skaters[m]['nameAndId'] not in centres[j]['nameAndId'] and skaters[m]['nameAndId'] not in defensemen[k]['nameAndId'] and skaters[m][
                #         0] not in wingers[l]['nameAndId'] and skaters[m]['nameAndId'] != winger['nameAndId']:
                weight = goalie['weight'] + centres[j]['weight'] + defensemen[k]['weight'] + wingers[l]['weight'] + \
                         util['weight']
                value = goalie['value'] + centres[j]['value'] + defensemen[k]['value'] + wingers[l]['value'] + util[
                    'value']
                index += 1

                if index % 10000 == 0:
                    # Only keep the top 100 sets of players
                    set_of_players = sorted(set_of_players, key=lambda tup: tup[10], reverse=True)
                    set_of_players = set_of_players[:10]

                if index % 10000000 == 0:
                    logging.info("Number of sets checked: " + str(index) + ", top set:")
                    logging.info(set_of_players[0])

                if weight <= limit:
                    full_set = [centres[j]['nameAndId'][0],
                                centres[j]['nameAndId'][1],
                                wingers[l]['nameAndId'][0],
                                wingers[l]['nameAndId'][1],
                                wingers[l]['nameAndId'][2],
                                defensemen[k]['nameAndId'][0],
                                defensemen[k]['nameAndId'][1],
                                goalie['nameAndId'],
                                util['nameAndId'],
                                weight,
                                value]
                    set_of_players.append(full_set)

    logging.debug("Number of sets checked: " + str(index))
    return set_of_players


def calculate_set_of_players(skaters, goalie, util, limit):
    # return brute_force(skaters, goalie, util, limit)  # , 100, 4000)
    return knapsack(skaters, goalie, util, limit)


if __name__ == '__main__':
    sqlite_file = 'daily_fantasy_hockey_db.sqlite'

    conn, c = connect(sqlite_file)
    # Need to turn on foreign keys, not on by default
    c.execute('''PRAGMA foreign_keys = ON''')

    logging.debug("Hardcoding date and goalies for the lineup....")
    date_for_lineup = datetime.date.today()# - datetime.timedelta(days=1)
    chosen_goalies = ["Jaroslav Halak (7724121)", "Tuukka Rask (7724113)"]#, "Ben Bishop (7724127)", "Roberto Luongo (7724126)"]

    # Create lineups for all combinations of top goalies (or chosen goalies) and top value/cost players
    # Write top lineups to file
    with open("../resources/DKLineup.csv", "w") as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerow(["C", "C", "W", "W", "W", "D", "D", "G", "UTIL"])

        logging.debug("Starting creating lineups....")
        final_lineups = []
        all_lineups = []

        logging.debug("Setting up players with ID and values....")
        players = get_player_data(c, date_for_lineup)
        conn.commit()
        unused_players = players
        limit = 500
        for i in range(len(chosen_goalies)):
            # for i in range(20):
            skaters = players
            # sorted_set_of_players = []
            for j in range(5):
                # Sort the players
                # skaters = sorted(skaters, key=lambda tup: tup['value'], reverse=True)

                # Pick a goalie and a Util (using costliest players, can do it based on value / cost ratio by using
                # tup['value'] / tup['weight'] as done with unused_players above
                # goalies = [item for item in unused_players if item['position'] == "G"]

                # Find the top goalies
                # goalies = sorted(goalies, key=lambda tup: tup['weight'], reverse=True)

                # Find the chosen goalies
                chosen_goalie = [item for item in unused_players if item['nameAndId'] == chosen_goalies[i]][0]
                # chosen_goalie = goalies[0]
                # del goalies[0]

                # Sort list of players and remove any goalies and players with value less than 1.5
                # Choose one Util from the from of the list
                skaters = [item for item in skaters if item['position'] != "G" and item['value'] > 1.5]
                skaters = sorted(skaters, key=lambda tup: tup['value'] / tup['weight'], reverse=True)
                chosen_util = skaters[0]

                # Remove Util from skaters (will be returned after one full loop)
                skaters = [item for item in skaters if item['nameAndId'] != chosen_util['nameAndId']]

                # Add random noise in order to get varied results
                # unused_players_with_noise = unused_players
                # for player in unused_players:
                #     player['value'] += random.uniform(-0.5, 0.5)

                # Re-sort the players, remove goalies
                # unused_players_with_noise = unused_players
                # unused_players_with_noise = sorted(unused_players_with_noise,
                #                                    key=lambda tup: tup['value'] / tup['weight'],
                #                                    reverse=True)
                # unused_players_with_noise = [item for item in unused_players if item['position'] != "G"]

                # skaters = [item for item in unused_players if item['position'] != "G"]
                logging.info("Getting lineup with " + chosen_goalie['nameAndId'] + " as goalie, and " + chosen_util[
                    'nameAndId'] + " as Util.")

                calculated_set_of_players = calculate_set_of_players(skaters, chosen_goalie, chosen_util, limit)
                calculated_set_of_players = sorted(calculated_set_of_players, key=lambda tup: tup[10], reverse=True)
                logging.debug(calculated_set_of_players[0])

                # Lower value of non-chosen players in selected set (C,W,D), as they've already been selected
                for player in skaters:
                    if player['nameAndId'] in calculated_set_of_players[0] and player['position'] in ["C", "W", "D"]:
                        logging.debug("Lowering value of " + str(player['nameAndId']) + " by 0.5.")
                        player['value'] -= 0.25

                # Write top lineup to csv
                writer.writerow(calculated_set_of_players[0][:9])
                csvfile.flush()

                # Add found lineup to all lineups
                all_lineups.append(calculated_set_of_players[0])
                # for s in range(len(calculated_set_of_players)):
                #    all_lineups.append(calculated_set_of_players[s])

            # Remove goalie from players
            players = [item for item in players if item['nameAndId'] != chosen_goalie['nameAndId']]

        csvfile.close()

    # Sort final lineups and print
    all_lineups = sorted(all_lineups, key=lambda tup: tup[10], reverse=True)
    for s in range(len(all_lineups)):
        logging.info(all_lineups[s])

    # Write all lineups to database
    insert_all_lineups(c, all_lineups, date_for_lineup)
    conn.commit()

    with open("../resources/DKAllLineups.csv", "w") as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerow(["C", "C", "W", "W", "W", "D", "D", "G", "UTIL", "Weight", "Value"])
        for s in range(len(all_lineups)):
            writer.writerow(all_lineups[s])

        csvfile.close()