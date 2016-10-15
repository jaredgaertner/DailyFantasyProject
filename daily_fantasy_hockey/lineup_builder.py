import csv
import datetime
import json
import urllib.request

from bhp.scripts.nhl_updates.nhl_game import nhl_game

__author__ = "jaredg"


# try:
#     xrange
# except:
#     xrange = range
 
# def totalvalue(comb, limit):
#     ' Totalise a particular combination of players'
#     totwt = totval = 0
#     for item, wt, val, position in comb:
#         totwt  += wt
#         totval += val
#     return (totval, totwt)
#
# def knapsack01_dp(players, limit):
#     table = [[0 for w in range(limit + 1)] for j in xrange(len(players) + 1)]
#
#     for j in xrange(1, len(players) + 1):
#         item, wt, val, position = players[j-1]
#         for w in xrange(1, limit + 1):
#             if wt > w:
#                 table[j][w] = table[j-1][w]
#             else:
#                 table[j][w] = max(table[j-1][w],
#                                   table[j-1][w-wt] + val)
#
#     result = []
#     w = limit
#     for j in range(len(players), 0, -1):
#         was_added = table[j][w] != table[j-1][w]
#
#         if was_added:
#             item, wt, val, position = players[j-1]
#             result.append(players[j-1])
#             w -= wt
#
#     return result


# Find up to max_size of players with maximum value to weight ratio
def find_goalie_util_pair(players, max_size = 50):
    #print(item)

    goalies = [item for item in players if item[3] == "G"]
    set_of_players = []
    for i in range(0, len(goalies)-1):
        for j in range(0, len(players)-1):
            if goalies[i] != players[j]:
                player_pair = [[goalies[i][0], players[j][0]], goalies[i][1] + players[j][1], goalies[i][2] + players[j][2], "G,Util"]
                set_of_players.append(player_pair)

    # Take the max_size number of optimal value to weight ratio and the max_size number of highest value pairs
    print("Number of " + set_of_players[0][3] + " pairs: " + str(len(set_of_players)))
    sorted_set_of_players_optimal = sorted(set_of_players, key=lambda tup: tup[2]/tup[1], reverse=True)
    sorted_set_of_players_highest_value = sorted(sorted_set_of_players_optimal[max_size:], key=lambda tup: tup[2], reverse=True)
    return sorted_set_of_players_optimal[:max_size] + sorted_set_of_players_highest_value[:max_size]

# Find up to max_size pairs of players with maximum value to weight ratio
def find_player_pair(players, position, max_size = 50):
    #print(item)
    players = [item for item in players if item[3] == position]
    set_of_players = []
    for i in range(0, len(players)-1):
        for j in range(i+1, len(players)-1):
            player_pair = [[players[i][0], players[j][0]], players[i][1] + players[j][1], players[i][2] + players[j][2], position]
            set_of_players.append(player_pair)

    # Take the max_size number of optimal value to weight ratio and the max_size number of highest value pairs of the rest left
    print("Number of " + position + " pairs: " + str(len(set_of_players)))
    sorted_set_of_players_optimal = sorted(set_of_players, key=lambda tup: tup[2]/tup[1], reverse=True)
    sorted_set_of_players_highest_value = sorted(sorted_set_of_players_optimal[max_size:], key=lambda tup: tup[2], reverse=True)
    return sorted_set_of_players_optimal[:max_size] + sorted_set_of_players_highest_value[:max_size]

def find_player_triples(players, position, max_size = 2500):
    players = [item for item in players if item[3] == position]
    # only include the first max_size players
    #players = players[:max_size]
    set_of_players = []
    for i in range(0, len(players)-1):
        for j in range(i+1, len(players)-1):
            for k in range(i+j+1, len(players)-1):
                player_triple = [[players[i][0], players[j][0], players[k][0]], players[i][1] + players[j][1] + players[k][1], players[i][2] + players[j][2] + players[k][2], position]
                set_of_players.append(player_triple)

    # Take the max_size number of optimal value to weight ratio and the max_size number of highest value triplets
    print("Number of " + position + " triples: " + str(len(set_of_players)))
    sorted_set_of_players_optimal = sorted(set_of_players, key=lambda tup: tup[2]/tup[1], reverse=True)
    sorted_set_of_players_highest_value = sorted(sorted_set_of_players_optimal[max_size:], key=lambda tup: tup[2], reverse=True)
    return sorted_set_of_players_optimal[:max_size] + sorted_set_of_players_highest_value[:max_size]

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

def get_goalie_value(name, date):
    return []

def get_player_value(name, date):
    return []

def get_player_value(intial_value, name, position):
    #url = 'https://statsapi.web.nhl.com/api/v1/schedule?startDate=2016-05-02&endDate=2016-05-07'
    #response = urllib.request.urlopen(url).read()
    #data = json.loads(response.decode())
    #print(data)

    #url = 'http://statsapi.web.nhl.com/api/v1/game/2015030213/feed/live?site=en_nhl'
    #response = urllib.request.urlopen(url).read()
    #data = json.loads(response.decode())
    #print(data)

    # select p.id, p.fullName, dkp.* from players p inner join games_draftkings_points dkp on p.id = dkp.playerId
    # where fullName = "Brent Burns"
    # order by dkp.gamePk desc

    if position == "G":
        return intial_value
    else:
        return intial_value

def brute_force(players, limit, value_total_min = 0, max_players_size = 100, max_set_size = 20):
    # Only take the best max_size players, based on weight to value ratio
    players = sorted(players, key=lambda tup: tup[2]/tup[1], reverse=True)
    players = players[:max_players_size]

    # Find each set of positions
    goalies = [item for item in players if item[3] == "G"]
    goalies = sorted(goalies, key=lambda tup: tup[2]/tup[1], reverse=True)
    print("Number of G: " + str(len(goalies)))
    utils = [item for item in players if item[3] != "G"]
    utils = sorted(utils, key=lambda tup: tup[2]/tup[1], reverse=True)
    print("Number of Util: " + str(len(utils)))
    defensemen = find_player_pair(players, "D", max_set_size)
    centres = find_player_pair(players, "C", max_set_size)
    wingers = find_player_triples(players, "W", max_set_size)

    set_of_players = []
    index = 0

    print("Potential number of combinations: " + str(len(goalies)*len(centres)*len(defensemen)*len(wingers)*len(utils)))
    for i in range(0, len(goalies)-1):
        for j in range(0, len(centres)-1):
            for k in range(0, len(defensemen)-1):
                for l in range(0, len(wingers)-1):
                    for m in range(0, len(utils)-1):
                        if utils[m][0] not in centres[j][0] and utils[m][0] not in defensemen[k][0] and utils[m][0] not in wingers[l][0]:
                            weight = goalies[i][1] + centres[j][1] + defensemen[k][1] + wingers[l][1] + utils[m][1]
                            value = goalies[i][2] + centres[j][2] + defensemen[k][2] + wingers[l][2] + utils[m][2]
                            index += 1
                            if index % 10000000 == 0:
                                print("Number of sets checked: " + str(index))

                            if weight <= limit and value > value_total_min:
                                full_set = [[goalies[i][0], centres[j][0], defensemen[k][0], wingers[l][0], utils[m][0]], weight, value, "all"]
                                #print(full_set)
                                set_of_players.append(full_set)
                                #if len(set_of_players) >= 1000:
                                #    print("Number of sets checked: " + str(index))
                                #    return set_of_players
    print("Number of sets checked: " + str(index))
    return set_of_players

with open("../resources/DKSalaries.csv") as csvfile:
    #games = Game.objects.filter(date=datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))
    #for g in games:
    #print("Parsing game: %s" % g.nhl_game_id)
    #print("%s, %s, %s" % ("02", str(g.nhl_game_id)[6:10], g.year.description))
    #n = nhl_game("03", "0213", "2015", True)
    #n.print_to_file(n.homeTeamSkaters, n.awayTeamSkaters)

    reader = csv.DictReader(csvfile)
    players = []
    limit = 50000
    #value_total_min = 0
    #max_players_size = 100
    #max_set_size = 20
    get_actual_value(datetime.date.today())
    for row in reader:
        #print(row['Position'], row['Name'], row['Salary'], row["GameInfo"], row['AvgPointsPerGame'])
        name = row['Name']
        weight = int(row['Salary'])
        intial_value = int(float(row['AvgPointsPerGame'])* 1000) if int(float(row['AvgPointsPerGame'])* 1000) != 0 else 100
        position = row['Position'] if row['Position'] != "LW" and row['Position'] != "RW" else "W"
        value = get_player_value(intial_value, row['Name'], position)
        item = [name, weight, value, position]
        players.append(item)
    print("Number of players: " + str(len(players)))
    bagged = brute_force(players, limit)#, value_total_min, max_players_size, max_set_size)
    sorted_set_of_players = sorted(bagged, key=lambda tup: tup[2], reverse=True)
    for s in range(0, min(len(sorted_set_of_players)-1, 10)):
        print(sorted_set_of_players[s])

    # Remove goalie from optimal solution, try again
    if len(sorted_set_of_players) > 0:
        print("Top goalie in optimal solution: " + str(sorted_set_of_players[0][0][0]))
        players = [item for item in players if item[0] != sorted_set_of_players[0][0][0]]
        bagged = brute_force(players, limit)#, value_total_min, max_players_size, max_set_size)
        sorted_set_of_players = sorted(bagged, key=lambda tup: tup[2], reverse=True)
        for s in range(0, min(len(sorted_set_of_players)-1, 10)):
            print(sorted_set_of_players[s])

    # Remove goalie from optimal solution, try again
    if len(sorted_set_of_players) > 0:
        print("Top goalie in optimal solution: " + str(sorted_set_of_players[0][0][0]))
        players = [item for item in players if item[0] != sorted_set_of_players[0][0][0]]
        bagged = brute_force(players, limit)#, value_total_min, max_players_size, max_set_size)
        sorted_set_of_players = sorted(bagged, key=lambda tup: tup[2], reverse=True)
        for s in range(0, min(len(sorted_set_of_players)-1, 10)):
            print(sorted_set_of_players[s])

    # Remove goalie from optimal solution, try again
    if len(sorted_set_of_players) > 0:
        print("Top goalie in optimal solution: " + str(sorted_set_of_players[0][0][0]))
        players = [item for item in players if item[0] != sorted_set_of_players[0][0][0]]
        bagged = brute_force(players, limit)#, value_total_min, max_players_size, max_set_size)
        sorted_set_of_players = sorted(bagged, key=lambda tup: tup[2], reverse=True)
        for s in range(0, min(len(sorted_set_of_players)-1, 10)):
            print(sorted_set_of_players[s])

    #goalie_util = find_goalie_util_pair(players)
    #print(goalie_util)
    #all_sets_of_players = []
    #all_sets_of_players.extend(goalie_util)
    #all_sets_of_players.extend(centres)
    #all_sets_of_players.extend(defensemen)
    #all_sets_of_players.extend(wingers)
    #sorted_all_sets_of_players = sorted(all_sets_of_players, key=lambda tup: tup[2]/tup[1], reverse=True)
    #print(sorted_all_sets_of_players)
    #bagged = multi_choice_knapsack(sorted_all_sets_of_players, limit)
    #bagged = knapsack01_dp(all_sets_of_players, limit)
    #print(bagged)
    #for i in range(1, len(bagged)):
    #    print(bagged[i])
    #    print("Bagged the following players\n  " + '\n  '.join(sorted(item for item,weight,value,position in bagged)))
    #    val, wt = totalvalue(bagged[i], limit)
    #    print("for a total value of %i and a total weight of %i" % (val, wt))

        
