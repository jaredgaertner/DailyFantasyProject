import datetime
import logging
from database import database
from calculate_lineups import calculate_lineups
from calculate_statistics import calculate_statistics
from update_game_info import update_game_info

__author__ = "jaredg"

# Configure logging
logFormatter = logging.Formatter(
    "%(asctime)s [%(threadName)s][%(levelname)s][%(filename)s:%(lineno)s - %(funcName)s()] %(message)s")
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler("../resources/logs/main.log." + datetime.datetime.today().strftime("%Y%m%d-%H%M%S"))
fileHandler.setFormatter(logFormatter)
# fileHandler.setLevel(logging.DEBUG)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
# consoleHandler.setLevel(logging.INFO)
rootLogger.addHandler(consoleHandler)
rootLogger.setLevel(logging.INFO)

if __name__ == '__main__':
    db = database()

    logging.debug("Hardcoding date and goalies for the lineup....")
    date_for_lineup = datetime.datetime.today()  # - datetime.timedelta(days=1)
    chosen_goalies = ["Cam Talbot (7745471)", "Semyon Varlamov (7745462)", "Braden Holtby (7745457)", "Keith Kinkaid (7745439)", "Anders Nilsson (7745440)"]
    lineups_per_goalie = 10
    ir_players = ["Blake Comeau (7745288)",
                  "Trevor van Riemsdyk (7745392)",
                  "Andrew MacDonald (7745335)",
                  "Jason Dickinson (7745156)",
                  "Nikita Soshnikov (7745255)",
                  "Jason Spezza (7745300)",
                  "Patrick Sharp (7745221)",
                  "Mattias Janmark (7745157)",
                  "Joffrey Lupul (7745245)",
                  "Tyler Motte (7745281)",
                  "Zach Bogosian (7745318)",
                  "Jason Spezza (7745300)",
                  "Tyler Ennis (7745162)",
                  "Mathieu Perreault (7745140)",
                  "Bryan Little (7745138)",
                  "Cody Eakin (7745227)",
                  "Drew Stafford (7745287)",
                  "Ales Hemsky (7745299)",
                  "Kris Russell (7745416)",
                  "Jack Eichel (7745101)"]

    # Update game data, if needed
    # update_game_info(db, "day_ago")

    # calculate all lineups/entries
    calculate_lineups(db, date_for_lineup, chosen_goalies, lineups_per_goalie, ir_players, "entry")

    # Get statistics from previous night
    calculate_statistics(db)

    db.close()