import datetime
import logging
from database import database
from calculate_lineups import calculate_lineups
from calculate_statistics import calculate_statistics
from update_game_info import *
from calculate_expected_values import *

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
    date_for_lineup = datetime.datetime.today()  # + datetime.timedelta(days=1)

    lineup_type = "initial"
    number_of_lineups = 20

    # Update game data, if needed
    # update_player(db, 8479423)
    # update_game_info(db, "day_ago")
    calculate_expected_values(db, False, "20192020")

    # calculate all lineups/entries
    calculate_lineups(db, date_for_lineup, number_of_lineups, lineup_type)

    # Get statistics from previous night
    if lineup_type == "initial":
        calculate_statistics(db)

    db.close()