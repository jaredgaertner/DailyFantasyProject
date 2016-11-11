import copy
import csv
import logging
import random
from entry import Entry
from lineup import Lineup
from player_draftkings_info import PlayerDraftKingsInfo
from knapsack import knapsack, brute_force

__author__ = "jaredg"

logger = logging.getLogger(__name__)

def calculate_sets_of_players(skaters, goalie, util, limit, type = "knapsack"):
    if type == "knapsack":
        return knapsack(skaters, goalie, util, limit)
    elif type == "brute_force":
        return brute_force(skaters, goalie, util, limit)  # , 100, 4000)
    else:
        raise ValueError("Invalid type for calculate_set_of_players: " + type + ", choose either knapsack or brute_force.")


def get_player_data(db, date_for_lineup, ir_players):
    players = []
    # Check if any IR players exist in database
    if ir_players:
        logging.debug("Checking IR players")
        db.query("select exists(select 1 from player_draftkings_info where nameAndId = ?) irPlayerInfoExists",
                  (ir_players[0],))
        if db.fetchone()['irPlayerInfoExists'] == 1:
            # Clear out table and recalculate values
            logging.info("Clearing out player info for IR for " + str(date_for_lineup))
            db.query("delete from player_draftkings_info where date(dateForLineup) = date(?)", (date_for_lineup,))
            db.commit()

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
        logging.info("Player information for DraftKings doesn't exist, grabbing from csv file: DKSalaries_" + date_for_lineup.strftime("%d%b%Y").upper() + ".csv")
        with open("../resources/DKSalaries_" + date_for_lineup.strftime("%d%b%Y").upper() + ".csv", "r") as csvfile:
            # Skip the first 7 lines, as it contains the format for uploading
            for i in range(7):
                next(csvfile)

            reader = csv.DictReader(csvfile)
            for row in reader:
                nameAndId = row['Name + ID']
                if nameAndId not in ir_players:
                    player_info = PlayerDraftKingsInfo(db,
                                                       nameAndId,
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
            logging.info("Entry information for DraftKings doesn't exist, trying to grab from csv file: DKEntries_" + date_for_lineup.strftime("%d%b%Y").upper() + ".csv")
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

def calculate_lineups(db, date_for_lineup, chosen_goalies, lineups_per_goalie, ir_players, lineup_type = "initial_lineup"):

    # Create lineups/entries for all combinations of top goalies (or chosen goalies) and top value/cost players
    # Write top lineups/entries to file
    if lineup_type == "initial_lineup":
        filename = "../resources/lineups/DKLineup_" + date_for_lineup.strftime("%Y%m%d-%H%M%S") + ".csv"
        header_row = ["C", "C", "W", "W", "W", "D", "D", "G", "UTIL"]
    elif lineup_type == "entry":
        filename = "../resources/lineups/DKEntries_" + date_for_lineup.strftime("%Y%m%d-%H%M%S") + ".csv"
        header_row = ["Entry ID", "Contest Name", "Contest ID", "Entry Fee", "C", "C", "W", "W", "W", "D", "D", "G", "UTIL"]
    else:
        raise ValueError("Invalid entry for lineup_type.")

    with open(filename, "w") as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerow(header_row)

        logging.debug("Starting creating lineups....")
        all_lineups = []

        logging.debug("Setting up players with ID and values....")
        players = get_player_data(db, date_for_lineup, ir_players)
        if lineup_type == "entry":
            entries = get_entries(db, date_for_lineup)
        else:
            entries = None

        limit = 500
        for i in range(len(chosen_goalies)):
            skaters = copy.deepcopy(players)
            # Use the following statements to check a specific player's value
            # ss_value = [item for item in skaters if item['nameAndId'] == 'Steven Stamkos (7723976)'][0]['value']
            # logging.debug("Steven Stamkos value: " + str(ss_value) + ", players length: " + str(len(players)))
            for j in range(lineups_per_goalie):
                # Find the chosen goalies
                chosen_goalie = [item for item in players if item.get_name_and_id() == chosen_goalies[i]][0]

                # Sort list of players and remove any goalies and players with value less than 1.5 and weight 25 or under
                # Choose one Util from the from of the list
                skaters = [item for item in skaters if item.get_position() != "G" and item.get_value() > 1.0 and item.get_weight() > 25]
                # skaters = sorted(skaters, key=lambda tup: tup['value'] / tup['weight'], reverse=True)
                skaters = sorted(skaters, key=lambda tup: tup.get_value(), reverse=True)
                chosen_util = skaters[0]

                # Remove Util from skaters (will be returned after one full loop)
                skaters = [item for item in skaters if item.get_name_and_id() != chosen_util.get_name_and_id()]

                # Add random noise in order to get varied results
                # unused_players_with_noise = unused_players
                for player in skaters:
                    player.add_value(random.uniform(-0.25, 0.25))

                # skaters = [item for item in unused_players if item['position'] != "G"]
                logging.info(
                    "Getting lineup with " + chosen_goalie.get_name_and_id() + " as goalie, and " + chosen_util.get_name_and_id() + " as Util.")

                calculated_set_of_players = calculate_sets_of_players(skaters, chosen_goalie, chosen_util, limit)
                calculated_set_of_players = sorted(calculated_set_of_players, key=lambda tup: tup[10], reverse=True)
                calculated_lineup = Lineup(db, calculated_set_of_players[0])
                logging.debug(calculated_lineup)

                # Lower value of non-chosen players in selected set (C,W,D), as they've already been selected
                for p in skaters:
                    if p.get_name_and_id() in calculated_set_of_players[0] and p.get_position() in ["C", "W", "D"]:
                        logging.debug("Lowering value of " + str(p.get_name_and_id()) + " by 0.25.")
                        p.add_value(-0.25)

                # Write top lineup to csv
                writer.writerow(calculated_set_of_players[0][:9])
                csvfile.flush()

                # Add found lineup to all lineups
                all_lineups.append(calculated_lineup)

            # Remove goalie from players
            players = [item for item in players if item.get_name_and_id() != chosen_goalie.get_name_and_id()]

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

    with open("../resources/lineups/DKAllLineups_" + date_for_lineup.strftime("%Y%m%d-%H%M%S") + ".csv", "w") as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerow(["C", "C", "W", "W", "W", "D", "D", "G", "UTIL", "Weight", "Value"])
        for s in range(len(all_lineups)):
            writer.writerow(all_lineups[s].get_list())

        csvfile.close()

    if lineup_type == "entry":
        with open("../resources/lineups/DKEntries_" + date_for_lineup.strftime("%Y%m%d-%H%M%S") + ".csv", "w") as csvfile:
            writer = csv.writer(csvfile, lineterminator='\n')
            writer.writerow(["Entry ID", "Contest Name", "Contest ID", "Entry Fee", "C", "C", "W", "W", "W", "D", "D", "G", "UTIL"])#, "Weight", "Value"])
            for s in range(len(entries)):
                writer.writerow(entries[s].get_list()[:13])

            csvfile.close()