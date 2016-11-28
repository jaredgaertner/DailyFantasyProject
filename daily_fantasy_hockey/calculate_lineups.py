import copy
import csv
import logging
import random
import re
import urllib
from bs4 import BeautifulSoup
from entry import Entry
from lineup import Lineup
from player_draftkings_info import PlayerDraftKingsInfo
from knapsack import knapsack, brute_force
from calculate_expected_values import get_all_active_player_ids

__author__ = "jaredg"

logger = logging.getLogger(__name__)


def calculate_sets_of_players(skaters, goalies, util, limit, type="knapsack"):
    if type == "knapsack":
        return knapsack(skaters, goalies, util, limit)
    elif type == "brute_force":
        return brute_force(skaters, goalies, util, limit)  # , 100, 4000)
    else:
        raise ValueError(
            "Invalid type for calculate_set_of_players: " + type + ", choose either knapsack or brute_force.")


def get_starting_goalies(db, date_for_lineup):
    starting_goalies = []
    try:
        logging.info("Finding starting goalies...")
        url = "http://www2.dailyfaceoff.com/starting-goalies/" + str(date_for_lineup.year) + "/" + str(
            date_for_lineup.month) + "/" + str(date_for_lineup.day) + "/"
        soup = BeautifulSoup(urllib.request.urlopen(url).read(), "html.parser")
        # matchups = soup.find(id="matchups")
        for row in soup.find_all("div", "goalie"):
            if row.find("h5") != None:
                goalie_name = row.h5.a.string
                logging.info("Goalie name: " + str(goalie_name))
                status = row.dl.dt.string
                logging.info("Status: " + str(status))

                starting_goalies.append(goalie_name)

            # Not currently needed, as only shows goalies which aren't confirmed
            # Else, we need to look at the document.write statement
            else:
                match = re.search(r"document\.write\(\"(.+)\"\)", str(row))
                goalie_info = BeautifulSoup(match.group(1), "html.parser")
                goalie_name = goalie_info.h5.a.string
                logging.info("Goalie name: " + str(goalie_name))
                status = goalie_info.dl.dt.string
                logging.info("Status: " + str(status))
                starting_goalies.append(goalie_name)

        return starting_goalies

    except Exception as e:
        logging.error("Could not connect to dailyfaceoff to get starting goalies.")
        logging.error("Got the following error:")
        logging.error(e)
        # Roll back any change if something goes wrong
        # db.rollback()
        raise e

    return starting_goalies


def get_player_data(db, date_for_lineup):  # , ir_players):
    players = []
    # Check if any IR players exist in database
    # if ir_players:
    #     logging.debug("Checking IR players")
    #     db.query("select exists(select 1 from player_draftkings_info where name = ?) irPlayerInfoExists",
    #               (ir_players[0],))
    #     if db.fetchone()['irPlayerInfoExists'] == 1:
    #         # Clear out table and recalculate values
    #         logging.info("Clearing out player info for IR for " + str(date_for_lineup))
    #         db.query("delete from player_draftkings_info where date(dateForLineup) = date(?)", (date_for_lineup,))
    #         db.commit()

    # Check if data already exists in database
    db.query("select exists(select 1 from player_draftkings_info where date(dateForLineup) = date(?)) playerInfoExists",
             (date_for_lineup,))
    if db.fetchone()['playerInfoExists'] == 1:
        logging.info("Player information for DraftKings already exists, grabbing from database.")
        try:

            db.query('''select pdi.id,
                                pdi.name,
                                pdi.nameAndId,
                                pdi.playerId,
                                pdi.weight,
                                pdi.value,
                                pdi.position,
                                pdi.gameInfo,
                                pdi.opponentId,
                                pdi.gamePk,
                                pdi.teamAbbrev,
                                pdi.dateForLineup,
                                pdi.draftType
                        from player_draftkings_info pdi
                        where date(pdi.dateForLineup) = date(?)''', (date_for_lineup,))
            for pdi in db.fetchall():
                player_info = PlayerDraftKingsInfo(db,
                                                   pdi['nameAndId'],
                                                   pdi['name'],
                                                   pdi['id'],
                                                   pdi['weight'],
                                                   pdi['position'],
                                                   pdi['gameInfo'],
                                                   pdi['teamAbbrev'],
                                                   pdi['draftType'],
                                                   pdi['dateForLineup'])
                players.append(player_info)
            return players

        except Exception as e:
            logging.error("Could not find player info for DraftKings on " + str(date_for_lineup))
            logging.error("Got the following error:")
            logging.error(e)
            raise e

    # Doesn't, so check from file
    else:
        logging.info(
            "Player information for DraftKings doesn't exist, grabbing from csv file: DKSalaries_" + date_for_lineup.strftime(
                "%d%b%Y").upper() + ".csv")
        with open("../resources/DKSalaries_" + date_for_lineup.strftime("%d%b%Y").upper() + ".csv", "r") as csvfile:
            # Skip the first 7 lines, as it contains the format for uploading
            for i in range(7):
                next(csvfile)

            reader = csv.DictReader(csvfile)
            for row in reader:
                name = row[' Name']
                # if name not in ir_players:
                player_info = PlayerDraftKingsInfo(db,
                                                   row['Name + ID'],
                                                   row[' Name'],
                                                   row[' ID'],
                                                   int(int(row[' Salary']) / 100),
                                                   row['Position'],
                                                   row['GameInfo'],
                                                   row['TeamAbbrev '],
                                                   "Standard",
                                                   date_for_lineup)
                logging.debug(player_info)
                players.append(player_info)
                player_info.insert_player_data()

            csvfile.close()
            return players


def get_entries(db, date_for_lineup):
    entries = []

    # Check if data already exists in database
    db.query("select exists(select 1 from daily_draftkings_entries where date(createdOn) = date(?)) entryExists",
             (date_for_lineup,))
    if db.fetchone()['entryExists'] == 1:
        logging.info("Entries for DraftKings already exists, grabbing from database.")
        try:

            db.query('''select dde.id,
                               dde.entryId,
                               dde.contestName,
                               dde.contestId,
                               dde.entryFee
                        from daily_draftkings_entries dde
                        where date(dde.createdOn) = date(?)''', (date_for_lineup,))
            for dde in db.fetchall():
                logging.debug(dde)
                entry_info = Entry(db,
                                   dde['entryId'],
                                   dde['contestName'],
                                   dde['contestId'],
                                   dde['entryFee'],
                                   dde['id'])
                entries.append(entry_info)
            return entries

        except Exception as e:
            logging.error("Could not find entries for DraftKings on " + str(date_for_lineup))
            logging.error("Got the following error:")
            logging.error(e)
            raise e

    # Doesn't, so check from file
    else:
        try:
            logging.info(
                "Entry information for DraftKings doesn't exist, trying to grab from csv file: DKEntries_" + date_for_lineup.strftime(
                    "%d%b%Y").upper() + ".csv")
            with open("../resources/DKEntries_" + date_for_lineup.strftime("%d%b%Y").upper() + ".csv", "r") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if row['Entry ID']:
                        logging.debug(row)
                        entry_info = Entry(db,
                                           row['Entry ID'],
                                           row['Contest Name'],
                                           row['Contest ID'],
                                           row['Entry Fee']);
                        entries.append(entry_info)
                        entry_info.insert_entry()

                csvfile.close()
                return entries
        except FileNotFoundError as e:
            logging.info("Did not find entries, could be the initial lineup on " + str(date_for_lineup))
            logging.info("Got the following error:")
            logging.info(e)
            return None


def calculate_lineups(db, date_for_lineup, number_of_lineups, lineup_type="initial"):
    # Create lineups/entries for all combinations of top goalies (or chosen goalies) and top value/cost players
    # Write top lineups/entries to file
    if lineup_type == "initial":
        filename = "../resources/lineups/DKLineup_" + date_for_lineup.strftime("%Y%m%d-%H%M%S") + ".csv"
        header_row = ["C", "C", "W", "W", "W", "D", "D", "G", "UTIL"]
    elif lineup_type == "entry":
        filename = "../resources/lineups/DKEntries_" + date_for_lineup.strftime("%Y%m%d-%H%M%S") + ".csv"
        header_row = ["Entry ID", "Contest Name", "Contest ID", "Entry Fee", "C", "C", "W", "W", "W", "D", "D", "G",
                      "UTIL"]
    else:
        raise ValueError("Invalid entry for lineup_type.")

    with open(filename, "w") as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerow(header_row)

        logging.debug("Starting creating lineups....")
        all_lineups = []

        logging.debug("Setting up players with ID and values....")
        players = get_player_data(db, date_for_lineup)
        if lineup_type == "entry":
            entries = get_entries(db, date_for_lineup)
        else:
            entries = None

        logging.debug("Finding starting goalies....")
        starting_goalies = get_starting_goalies(db, date_for_lineup)
        goalies = [item for item in players if item.get_name() in starting_goalies]
        logging.debug(goalies)
        if len(goalies) == 0:
            raise ValueError("Could not find any starting goalies.")

        # Sort list of players and remove any goalies and players with value less than 1.0 and weight 25 or under, or if not active
        # Choose one Util from the from of the list
        logging.debug("Finding skaters....")
        skaters = copy.deepcopy(players)
        active_players = get_all_active_player_ids(db)
        skaters = [item for item in skaters if
                   item.get_position() != "G" and
                   item.get_value() > 1.0 and
                   item.get_weight() > 25 and
                   item.get_player_id() != None and
                   item.get_player_id() in active_players]
        # for skater in skaters:
        #     logging.info(skater)
        #     logging.info(skater.get_player_id())

        limit = 500
        # for i in range(len(chosen_goalies)):
        # skaters = copy.deepcopy(players)
        # Use the following statements to check a specific player's value
        # ss_value = [item for item in skaters if item['nameAndId'] == 'Steven Stamkos (7723976)'][0]['value']
        # logging.debug("Steven Stamkos value: " + str(ss_value) + ", players length: " + str(len(players)))
        for i in range(number_of_lineups):
            # Find the chosen goalies
            # chosen_goalie = [item for item in players if item.get_name_and_id() == chosen_goalies[i]][0]

            # Add random noise in order to get varied results
            # unused_players_with_noise = unused_players
            for skater in skaters:
                skater.add_value(random.uniform(-0.25, 0.25))

            # Choose a Util based on the best value
            skaters = sorted(skaters, key=lambda tup: tup.get_value(), reverse=True)
            chosen_util = skaters[0]

            # Remove Util from skaters (will be returned after calculating the set)
            skaters = [item for item in skaters if item.get_name_and_id() != chosen_util.get_name_and_id()]

            # skaters = [item for item in unused_players if item['position'] != "G"]
            logging.info("Getting lineup with " + chosen_util.get_name_and_id() + " as Util.")

            calculated_set_of_players = calculate_sets_of_players(skaters, goalies, chosen_util, limit)
            calculated_set_of_players = sorted(calculated_set_of_players, key=lambda tup: tup[10], reverse=True)
            calculated_lineup = Lineup(db, calculated_set_of_players[0])
            logging.debug(calculated_lineup)

            # Add Util back in for next loop
            skaters.append(chosen_util)

            # Lower value of non-chosen players in selected set (C,W,D), as they've already been selected
            for skater in skaters:
                if skater.get_name_and_id() in calculated_set_of_players[0]:
                    logging.info("Lowering value of " + str(skater.get_name_and_id()) + " by 0.25.")
                    skater.add_value(-0.25)

            for goalie in goalies:
                if goalie.get_name_and_id() in calculated_set_of_players[0]:
                    logging.info("Lowering value of " + str(goalie.get_name_and_id()) + " by 0.25.")
                    goalie.add_value(-0.25)

            # Write top lineup to csv
            writer.writerow(calculated_set_of_players[0][:9])
            csvfile.flush()

            # Add found lineup to all lineups
            logging.info(calculated_lineup)
            all_lineups.append(calculated_lineup)

            # Remove goalie from players
            # players = [item for item in players if item.get_name_and_id() != chosen_goalie.get_name_and_id()]

        csvfile.close()

    # Sort final lineups, add to entries, and print
    all_lineups = sorted(all_lineups, key=lambda tup: tup.get_total_value(), reverse=True)
    for s in range(len(all_lineups)):
        if lineup_type == "entry":
            entries[s].set_lineup(all_lineups[s])
            logging.info(entries[s].get_list())
        else:
            logging.info(all_lineups[s].get_list())

    # Write all lineups to database
    for lineup in all_lineups:
        lineup.insert_lineup()

    with open("../resources/lineups/DKAllLineups_" + date_for_lineup.strftime("%Y%m%d-%H%M%S") + ".csv",
              "w") as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerow(["C", "C", "W", "W", "W", "D", "D", "G", "UTIL", "Weight", "Value"])
        for s in range(len(all_lineups)):
            writer.writerow(all_lineups[s].get_list())

        csvfile.close()

    if lineup_type == "entry":
        with open("../resources/lineups/DKEntries_" + date_for_lineup.strftime("%Y%m%d-%H%M%S") + ".csv",
                  "w") as csvfile:
            writer = csv.writer(csvfile, lineterminator='\n')
            writer.writerow(
                ["Entry ID", "Contest Name", "Contest ID", "Entry Fee", "C", "C", "W", "W", "W", "D", "D", "G",
                 "UTIL"])  # , "Weight", "Value"])
            for s in range(len(entries)):
                writer.writerow(entries[s].get_list()[:13])

            csvfile.close()
