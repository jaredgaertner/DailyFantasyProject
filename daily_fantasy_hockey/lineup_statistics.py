import sqlite3
import time
import datetime
import logging
from load_game_data import load_game_data
from database import database

__author__ = "jaredg"

# Configure logging
logFormatter = logging.Formatter(
    "%(asctime)s [%(threadName)s][%(levelname)s][%(filename)s:%(lineno)s - %(funcName)s()] %(message)s")
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler("../resources/logs/lineup_statistics.log." + time.strftime("%Y%m%d-%H%M%S"))
fileHandler.setFormatter(logFormatter)
# fileHandler.setLevel(logging.DEBUG)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
# consoleHandler.setLevel(logging.INFO)
rootLogger.addHandler(consoleHandler)
rootLogger.setLevel(logging.INFO)

def get_actual_player_value(db, playerId):
    try:
        logging.debug("Getting player value for player ID: " + str(playerId))
        db.query('''select p.fullName, gdp.points
                      from games_draftkings_points gdp
                     inner join players p
                        on gdp.playerid = p.id
                     inner join games g
                        on gdp.gamePk = g.gamePk
                     where p.id = ? and
                           datetime(?) between datetime(g.gameDate,'-12 hours') and
                           datetime(g.gameDate, '+12 hours')''', (playerId,datetime.datetime.today(),))

        for player_stats in db.fetchall():
            logging.debug(player_stats)
            logging.info("Got value of " + str(player_stats['points']) + " for " + str(player_stats['fullName']))
            return float(player_stats['points'])

        # Didin't find any values (didn't play), return 0
        logging.debug("Got value of 0 for " + str(playerId))
        return 0

    except Exception as e:
        logging.error("Could not find player points for DraftKings:")
        logging.error("Got the following error:")
        logging.error(e)
        raise LookupError("Error when trying to find value.")


def get_actual_lineup_value(db, lineup):
    logging.debug("Getting actual lineup value.")
    logging.debug(lineup)
    try:
        total_value = 0.0
        for playerId in lineup:
            total_value += get_actual_player_value(db, playerId)
        return total_value

    except Exception as e:
        logging.error("Could not get lineup value")
        logging.error(lineup)
        logging.error("Got the following error:")
        logging.error(e)
        raise e


def update_lineup_actual_values(db):
    logging.debug("Finding actual value for player lineups for DraftKings")
    try:
        db.query('''select ddl.id,
                            ddl.centre1,
                            ddl.centre2,
                            ddl.winger1,
                            ddl.winger2,
                            ddl.winger3,
                            ddl.defence1,
                            ddl.defence2,
                            ddl.goalie,
                            ddl.util,
                            ddl.gameDate
                       from daily_draftkings_lineups ddl
                      where ddl.actualValue is null''')
        for daily_lineup in db.fetchall():
            logging.debug("Updating daily lineup:")
            logging.debug(daily_lineup)
            lineup = [daily_lineup['centre1'], daily_lineup['centre2'], daily_lineup['winger1'],
                      daily_lineup['winger2'], daily_lineup['winger3'], daily_lineup['defence1'],
                      daily_lineup['defence2'], daily_lineup['goalie'], daily_lineup['util'],]
            actualValue = get_actual_lineup_value(db, lineup)

            logging.info("Total actual value " + str(actualValue))
            logging.debug(lineup)
            if actualValue > 0:
                db.query('''UPDATE OR IGNORE daily_draftkings_lineups
                             set actualValue = ?,
                                 updatedOn = ?
                             where id = ?''', (actualValue, datetime.datetime.today(), daily_lineup['id'],))
            else:
                logging.error("Calculation for actual value was 0, error in calculation.")

    except Exception as e:
        logging.error("Could not update actual value for lineups for DraftKings:")
        logging.error("Got the following error:")
        logging.error(e)
        raise e


if __name__ == '__main__':
    db = database()

    # Update actual lineup values for any previous
    logging.info("Updating stats from previous day...")
    load_game_data(db, "day_ago")

    logging.info("Updating actual value for player lineups for DraftKings....")
    try:
        update_lineup_actual_values(db)
        # find_optimal_lineups(c)
        db.close()

    except Exception as e:
        logging.error("Could not update actual value for lineups for DraftKings:")
        logging.error("Got the following error:")
        logging.error(e)
        db.rollback()
        exit()
