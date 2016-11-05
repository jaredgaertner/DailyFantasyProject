import csv
import datetime
import json
import sqlite3
import urllib.request

__author__ = "jaredg"


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


# Find up to max_size of players with maximum value to weight ratio
def find_goalie_util_pair(players, max_set_size=50):
    # print(item)

    goalies = [item for item in players if item[3] == "G"]
    set_of_players = []
    for i in range(0, len(goalies) - 1):
        for j in range(0, len(players) - 1):
            if goalies[i] != players[j]:
                player_pair = [[goalies[i][0], players[j][0]], goalies[i][1] + players[j][1],
                               goalies[i][2] + players[j][2], "G,Util"]
                set_of_players.append(player_pair)

    # Take the max_size number of optimal value to weight ratio and the max_size number of highest value pairs
    # print("Total number of " + set_of_players[0][3] + " pairs: " + str(len(set_of_players)))
    sorted_set_of_players_optimal = sorted(set_of_players, key=lambda tup: tup[2] / tup[1], reverse=True)
    sorted_set_of_players_highest_value = sorted(sorted_set_of_players_optimal[max_set_size:], key=lambda tup: tup[2],
                                                 reverse=True)
    return sorted_set_of_players_optimal[:max_set_size] + sorted_set_of_players_highest_value[:max_set_size]


# Find up to max_size pairs of players with maximum value to weight ratio
def find_player_pair(players, position, max_set_size=50):
    # print(item)
    players = [item for item in players if item[3] == position]
    set_of_players = []
    for i in range(0, len(players) - 1):
        for j in range(i + 1, len(players) - 1):
            player_pair = [[players[i][0], players[j][0]], players[i][1] + players[j][1], players[i][2] + players[j][2],
                           position]
            set_of_players.append(player_pair)

    # Take the max_size number of optimal value to weight ratio and the max_size number of highest value pairs of the rest left
    # print("Total number of " + position + " pairs: " + str(len(set_of_players)))
    sorted_set_of_players_optimal = sorted(set_of_players, key=lambda tup: tup[2] / tup[1], reverse=True)
    sorted_set_of_players_highest_value = sorted(sorted_set_of_players_optimal[max_set_size:], key=lambda tup: tup[2],
                                                 reverse=True)
    return sorted_set_of_players_optimal[:max_set_size] + sorted_set_of_players_highest_value[:max_set_size]


def find_player_triples(players, position, max_triple_set_size=2500):
    players = [item for item in players if item[3] == position]

    set_of_players = []
    for i in range(0, len(players) - 1):
        for j in range(i + 1, len(players) - 1):
            for k in range(i + j + 1, len(players) - 1):
                player_triple = [[players[i][0], players[j][0], players[k][0]],
                                 players[i][1] + players[j][1] + players[k][1],
                                 players[i][2] + players[j][2] + players[k][2], position]
                set_of_players.append(player_triple)

    # Take the max_size number of optimal value to weight ratio and the max_size number of highest value triplets
    # print("Total number of " + position + " triples: " + str(len(set_of_players)))
    sorted_set_of_players_optimal = sorted(set_of_players, key=lambda tup: tup[2] / tup[1], reverse=True)
    sorted_set_of_players_highest_value = sorted(sorted_set_of_players_optimal[max_triple_set_size:],
                                                 key=lambda tup: tup[2], reverse=True)
    return sorted_set_of_players_optimal[:max_triple_set_size] + sorted_set_of_players_highest_value[
                                                                 :max_triple_set_size]


# http://stackoverflow.com/questions/19389931/knapsack-constraint-python
# def multi_choice_knapsack(players, limit):
#     table = [[0 for w in range(limit + 1)] for j in xrange(len(players) + 1)]
#     no_of_positions = {"W":1,"C":1,"D":1,"G":1}
#     result = []
#     for j in xrange(1, len(players) + 1):
#         item, wt, val, position = players[j-1]
#         totval, totwt = totalvalue(result, limit)
#         if no_of_positions[position] > 0 and totwt + wt <= limit:
#             result.append(players[j-1])
#             no_of_positions[position] -= 1
#     return result

def get_player_value(name):
    try:
        c.execute('''select p.id, p.fullName,
                          AVG(case when g.gamePk like '2016%' then gdp.points else null end) AS average_points_for_year,
                          ifnull(AVG(case when g.gameDate > date('now','-7 day') then gdp.points else null end),0) AS average_points_for_week
                    from games_draftkings_points gdp
                    inner join players p
                    on gdp.playerid = p.id
                    inner join games g
                    on gdp.gamePk = g.gamePk
                    where (g.gamePk like '2016%' or
                          g.gameDate > date('now','-7 day')) and
                          p.fullName = ?
                    group by p.id, p.fullName
                    order by p.fullName''', (name,))
        value = 0
        for player_stats in c.fetchall():
            value = 0.75 * player_stats['average_points_for_year'] + 0.25 * player_stats['average_points_for_week']

        return value

    except Exception as e:
        print("Could not find player points for DraftKings:")
        print(player_stats)
        print("Got the following error:")
        print(e)
        return 0


# def totalvalue(comb, limit):
#     ' Totalise a particular combination of players'
#     totwt = totval = 0
#     for item, wt, val, position in comb:
#         totwt  += wt
#         totval += val
#     return (totval, totwt)

def knapsack01_dp(player_set, limit):
    table = [[0 for w in range(limit + 1)] for j in range(len(player_set) + 1)]

    print("Knapsack: Going through all " + str(len(player_set)) + " sets of players.")
    for j in range(1, len(player_set) + 1):
        item, wt, val, position = player_set[j - 1]
        # print("Item: " + str(item) + ", wt: " + str(wt) + ", val: " + str(val) + ", position: " + str(position))
        if j % 1000 == 0:
            print("Knapsack: Through " + str(j) + " of " + str(len(player_set)) + " items.")

        for w in range(1, limit + 1):
            if wt > w:
                table[j][w] = table[j - 1][w]
            else:
                table[j][w] = max(table[j - 1][w],
                                  table[j - 1][w - wt] + val)

    result = []
    w = limit
    for j in range(len(player_set), 0, -1):
        was_added = table[j][w] != table[j - 1][w]

        if was_added:
            item, wt, val, position = player_set[j - 1]
            result.append(player_set[j - 1])
            w -= wt

    return result


def multi_choice_knapsack(players, goalie, player_set, limit):
    positions = ["G", "D", "C", "W", "Util"]
    table = [[0 for w in range(limit + 1)] for j in range(len(positions) + 1)]
    player_added = [[0 for w in range(limit + 1)] for j in range(len(positions) + 1)]
    print("Knapsack: Going through all " + str(len(positions)) + " positions.")
    result = set()
    for i in range(1, len(positions) + 1):
        # item, wt, val, position = player_set[j-1]
        # print("Item: " + str(item) + ", wt: " + str(wt) + ", val: " + str(val) + ", position: " + str(position))
        print("Multiple Choice Knapsack: Checking position " + str(positions[i - 1]))
        if positions[i - 1] == "Util":
            current_player_set = [item for item in players if item[3] != "G"]
        elif positions[i - 1] == "G":
            current_player_set = [goalie]
        else:
            current_player_set = [item for item in player_set if item[3] == positions[i - 1]]

        for w in range(1, limit + 1):
            max_val_for_position = table[i - 1][w]
            for player in current_player_set:
                # find the max for all player_set of that position
                item, wt, val, position = player
                # print("Item: " + str(item) + ", wt: " + str(wt) + ", val: " + str(val) + ", position: " + str(position))
                if wt <= w and table[i - 1][w - wt] + val > max_val_for_position:
                    max_val_for_position = table[i - 1][w - wt] + val
                    player_added[i][w] = player
            table[i][w] = max_val_for_position

    result = []
    w = limit
    # print(table[i][w])
    total_value = 0
    total_weight = 0
    for i in range(len(positions), 0, -1):
        for j in range(w, 0, -1):
            was_added = table[i][j - 1] != table[i][j]

            if was_added:
                item, wt, val, position = player_added[i][j]
                result.append(player_added[i][j])
                total_value += val
                total_weight += wt
                w -= wt
                break;

                # Check change in value for each position added, find player with similar values
                # value_added = table[i][w] - table[i - 1][w]
                # players_added = [item for item in players if item[2] == value_added]
                # print(players_added)
                # result.append(players_added)

    print(total_value)
    print(total_weight)
    # print(result)
    return result


def knapsack(players, goalie, limit, max_util_size=200, max_set_size=20, max_triple_set_size=2500):
    # Only take the best max_goalies_size goalies, based on weight to value ratio
    # goalies = [item for item in players if item[3] == "G"]
    # goalies = sorted(goalies, key=lambda tup: tup[2]/tup[1], reverse=True)
    # goalies = goalies[:max_goalies_size]

    utils = [item for item in players if item[3] != "G"]
    utils = sorted(utils, key=lambda tup: tup[2] / tup[1], reverse=True)
    utils = utils[:max_util_size]

    defensemen = find_player_pair(players, "D", max_set_size)
    centres = find_player_pair(players, "C", max_set_size)
    wingers = find_player_triples(players, "W", max_triple_set_size)

    # print("Number of G being checked: " + str(len(goalies)))
    # print("Number of Util being checked: " + str(len(utils)))
    # print("Number of D pairs being checked: " + str(len(defensemen)))
    # print("Number of C pairs being checked: " + str(len(centres)))
    # print("Number of W pairs being checked: " + str(len(wingers)))

    # Create sets of objects, include pairs and triples, and pass into a knapsack solver
    player_set = utils  # goalies
    # for util in utils:
    #     player_set.append(util)

    for defensemen_pair in defensemen:
        player_set.append(defensemen_pair)

    for centre_pair in centres:
        player_set.append(centre_pair)

    for winger_triple in wingers:
        player_set.append(winger_triple)

    player_set = sorted(player_set, key=lambda tup: tup[2] / tup[1], reverse=True)
    # print(player_set)
    return multi_choice_knapsack(players, goalie, player_set, limit)


def brute_force(players, goalie, util, limit, max_set_size=20, max_triple_set_size=2500):
    defensemen = find_player_pair(players, "D", max_set_size)
    centres = find_player_pair(players, "C", max_set_size)
    wingers = find_player_triples(players, "W", max_triple_set_size)

    # print("Number of G being checked: " + str(len(goalies)))
    # print("Number of Util being checked: " + str(len(utils)))
    # print("Number of D pairs being checked: " + str(len(defensemen)))
    # print("Number of C pairs being checked: " + str(len(centres)))
    # print("Number of W pairs being checked: " + str(len(wingers)))

    set_of_players = []
    index = 0

    # print("Potential number of combinations: " + str(len(centres)*len(defensemen)*len(wingers)))#*len(utils)))
    # for i in range(0, len(goalies)-1):
    for j in range(0, len(centres) - 1):
        for k in range(0, len(defensemen) - 1):
            for l in range(0, len(wingers) - 1):
                # for m in range(0, len(utils)-1):
                # if utils[m][0] not in centres[j][0] and utils[m][0] not in defensemen[k][0] and utils[m][0] not in wingers[l][0]:
                weight = goalie[1] + centres[j][1] + defensemen[k][1] + wingers[l][1] + util[1]
                value = goalie[2] + centres[j][2] + defensemen[k][2] + wingers[l][2] + util[2]
                index += 1
                # if index % 10000000 == 0:
                #     print("Number of sets checked: " + str(index))

                if index % 10000 == 0:
                    # Only keep the top 100 sets of players
                    set_of_players = sorted(set_of_players, key=lambda tup: tup[2], reverse=True)
                    set_of_players = set_of_players[:10]

                if weight <= limit:
                    full_set = [centres[j][0][0], centres[j][0][1],
                                wingers[l][0][0], wingers[l][0][1], wingers[l][0][2], defensemen[k][0][0],
                                defensemen[k][0][1], goalie[0], util[0], weight, value]
                    set_of_players.append(full_set)

    # print("Number of sets checked: " + str(index))
    return set_of_players


def calculate_set_of_players(players, goalie, util, limit):
    return brute_force(players, goalie, util, limit)  # , 100, 4000)
    # return knapsack(players, goalies[0], limit)


if __name__ == '__main__':
    sqlite_file = 'daily_fantasy_hockey_db.sqlite'

    conn, c = connect(sqlite_file)
    # Need to turn on foreign keys, not on by default
    c.execute('''PRAGMA foreign_keys = ON''')

    # Set players with ID based on player_values
    print("Setting up players with ID and values....")
    players = []
    limit = 500
    with open("../resources/DKSalaries.csv", "r") as csvfile:
        # Skip the first 7 lines, as it contains the format for uploading
        for i in range(7):
            next(csvfile)

        reader = csv.DictReader(csvfile)
        for row in reader:
            # print(row)
            name = row[' Name']
            name_id = row['Name + ID']
            weight = int(int(row[' Salary']) / 100)
            value = get_player_value(name)
            position = row['Position'] if row['Position'] != "LW" and row['Position'] != "RW" else "W"
            item = [name_id, weight, value, position]
            players.append(item)
            # print(item)

    # Create lineups for all combinations of top five goalies and top five players
    print("Starting creating lineups....")
    unused_players = players
    final_lineups = []
    for i in range(2):
        unused_players = players
        sorted_set_of_players = []
        for j in range(2):
            # Sort the players
            unused_players = sorted(unused_players, key=lambda tup: tup[2] / tup[1], reverse=True)

            # Pick a goalie and a Util (using costliest players, can do it based on value / cost ratio by using
            # tup[2] / tup[1] as done with unused_players above
            goalies = [item for item in unused_players if item[3] == "G"]
            goalies = sorted(goalies, key=lambda tup: tup[1], reverse=True)
            # goalies = goalies[:max_goalies_size]

            utils = [item for item in unused_players if item[3] != "G"]
            utils = sorted(utils, key=lambda tup: tup[1], reverse=True)
            # utils = utils[:max_util_size]

            # Remove Util from unused_players (will be returned after one full loop)
            unused_players = [item for item in unused_players if item[0] != utils[0][0]]

            print("Getting lineup with " + goalies[0][0] + " as goalie, and " + utils[0][0] + " as Util.")
            calculated_set_of_players = calculate_set_of_players(unused_players[:400], goalies[0], utils[0], limit)
            # sorted_set_of_players = sorted(calculated_set_of_players, key=lambda tup: tup[2], reverse=True)
            for s in range(0, min(len(calculated_set_of_players) - 1, 10)):
                # print(sorted_set_of_players[s])
                final_lineups.append(calculated_set_of_players[s])

        # Remove goalie from players
        players = [item for item in players if item[0] != goalies[0][0]]

    # Sort final lineups and print
    final_lineups = sorted(final_lineups, key=lambda tup: tup[10], reverse=True)
    for s in range(0, len(final_lineups) - 1):
        print(final_lineups[s])

    # Write top lineups to file
    with open("../resources/DKLineup.csv", "w") as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerow(["C", "C", "W", "W", "W", "D", "D", "G", "UTIL"])
        for s in range(min(len(final_lineups), 5)):
            writer.writerow(final_lineups[s][:9])
