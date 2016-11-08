import sqlite3
import time
import datetime
import logging
from load_game_data import load_game_data

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


def get_actual_player_value(playerId, gameDate):
    try:
        logging.debug("Getting player value for player ID: " + str(playerId) + " for: " + str(gameDate))
        c.execute('''select gdp.points
                     from games_draftkings_points gdp
                     inner join players p
                     on gdp.playerid = p.id
                     inner join games g
                     on gdp.gamePk = g.gamePk
                     where p.id = ?
                     order by g.gameDate desc''', (playerId,))
                           # and
                           # datetime(?) between datetime(g.gameDate,'-1 day') and
                           # date(g.gameDate, '+1 day')''', (playerId, gameDate,))

        for player_stats in c.fetchall():
            logging.debug(player_stats)
            logging.debug("Got value of " + str(player_stats['points']))
            return int(player_stats['points'])

    except Exception as e:
        logging.error("Could not find player points for DraftKings:")
        logging.error("Got the following error:")
        logging.error(e)
        return 0


def get_actual_lineup_value(lineup, gameDate):
    logging.debug("Getting actual lineup value.")
    logging.debug(lineup)
    try:
        total_value = 0
        for playerId in lineup:
            playerValue = get_actual_player_value(playerId, gameDate)
            logging.debug(playerValue)
            total_value += playerValue
        logging.debug(total_value)
        return total_value

    except Exception as e:
        logging.error("Could not get lineup value")
        logging.error(lineup)
        logging.error("Got the following error:")
        logging.error(e)
        return 0

def update_lineup_actual_values(c):
    logging.debug("Finding actual value for player lineups for DraftKings")
    try:
        c.execute('''select ddl.id,
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
                      where ddl.actualValue is null and
                            ddl.gameDate < ?''', (datetime.date.today(),))
        for daily_lineup in c.fetchall():
            logging.debug("Updating daily lineup:")
            logging.debug(daily_lineup)
            lineup = [daily_lineup['centre1'], daily_lineup['centre2'], daily_lineup['winger1'],
                      daily_lineup['winger2'], daily_lineup['winger3'], daily_lineup['defence1'],
                      daily_lineup['defence2'], daily_lineup['goalie'], daily_lineup['util'],]
            actualValue = get_actual_lineup_value(lineup, daily_lineup['gameDate'])
            if actualValue > 0:
                c.execute('''UPDATE OR IGNORE daily_draftkings_lineups
                             set actualValue = ?
                             where id = ?''', (actualValue, daily_lineup['id']))
            else:
                logging.error("Calculation for actual value was 0, error in calculation.")

    except Exception as e:
        logging.error("Could not update actual value for lineups for DraftKings:")
        logging.error("Got the following error:")
        logging.error(e)
        # Roll back any change if something goes wrong
        # db.rollback()
        # raise e


if __name__ == '__main__':
    sqlite_file = 'daily_fantasy_hockey_db.sqlite'

    conn, c = connect(sqlite_file)
    # Need to turn on foreign keys, not on by default
    c.execute('''PRAGMA foreign_keys = ON''')

    # Update actual lineup values for any previous
    # logging.info("Updating stats from previous day...")
    load_game_data(conn, c, "week_ago")

    logging.info("Updating actual value for player lineups for DraftKings....")
    update_lineup_actual_values(c)
    # find_optimal_lineups(c)
    conn.commit()
