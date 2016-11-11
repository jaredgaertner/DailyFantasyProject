import datetime
import logging

__author__ = "jaredg"

logger = logging.getLogger(__name__)

def get_player_value(db, playerId, gamePkStart, gamePkEnd):
    try:
        logging.debug("Getting player value for player ID: " + str(playerId))
        db.query('''select p.fullName, gdp.points
                      from games_draftkings_points gdp
                     inner join players p
                        on gdp.playerid = p.id
                     inner join games g
                        on gdp.gamePk = g.gamePk
                     where p.id = ? and
                           gdp.gamePk between ? and
                           ?''', (playerId, gamePkStart, gamePkEnd, ))

        for player_stats in db.fetchall():
            logging.debug(player_stats)
            logging.debug("Got value of " + str(player_stats['points']) + " for " + str(player_stats['fullName']))
            return float(player_stats['points'])

        # Didin't find any values (didn't play), return 0
        logging.debug("Got value of 0 for " + str(playerId))
        return 0

    except Exception as e:
        logging.error("Could not find player points for DraftKings:")
        logging.error("Got the following error:")
        logging.error(e)
        raise LookupError("Error when trying to find value.")


def get_lineup_value(db, lineup, gamePkStart, gamePkEnd):
    logging.debug("Getting actual lineup value.")
    logging.debug(lineup)
    try:
        total_value = 0.0
        for playerId in lineup:
            total_value += get_player_value(db, playerId, gamePkStart, gamePkEnd)
        return total_value

    except Exception as e:
        logging.error("Could not get lineup value")
        logging.error(lineup)
        logging.error("Got the following error:")
        logging.error(e)
        raise e


def update_lineup_values(db):
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
                            ddl.gamePkStart,
                            ddl.gamePkEnd,
                            ddl.createdOn
                       from daily_draftkings_lineups ddl
                      where ddl.actualValue is null''')
        for daily_lineup in db.fetchall():
            logging.debug("Updating daily lineup:")
            logging.debug(daily_lineup)
            lineup = [daily_lineup['centre1'], daily_lineup['centre2'], daily_lineup['winger1'],
                      daily_lineup['winger2'], daily_lineup['winger3'], daily_lineup['defence1'],
                      daily_lineup['defence2'], daily_lineup['goalie'], daily_lineup['util'],]
            actualValue = get_lineup_value(db, lineup, daily_lineup['gamePkStart'], daily_lineup['gamePkEnd'])

            logging.debug("Total actual value " + str(actualValue))
            logging.debug(lineup)
            if actualValue > 0:
                db.query('''UPDATE OR IGNORE daily_draftkings_lineups
                             set actualValue = ?,
                                 updatedOn = ?
                             where id = ?''', (actualValue, datetime.datetime.today(), daily_lineup['id'],))
            else:
                logging.error("Calculation for total value was 0, error in calculation or game hasn't been completed.")

    except Exception as e:
        logging.error("Could not update actual value for lineups for DraftKings:")
        logging.error("Got the following error:")
        logging.error(e)
        raise e


def update_entry_values(db):
    logging.debug("Finding entry values for player lineups for DraftKings")
    try:
        db.query('''select dde.id,
                            dde.centre1,
                            dde.centre2,
                            dde.winger1,
                            dde.winger2,
                            dde.winger3,
                            dde.defence1,
                            dde.defence2,
                            dde.goalie,
                            dde.util,
                            dde.gamePkStart,
                            dde.gamePkEnd
                       from daily_draftkings_entries dde
                      where dde.actualValue is null''')
        for daily_entry in db.fetchall():
            logging.debug("Updating daily entry:")
            logging.debug(daily_entry)
            lineup = [daily_entry['centre1'], daily_entry['centre2'], daily_entry['winger1'],
                      daily_entry['winger2'], daily_entry['winger3'], daily_entry['defence1'],
                      daily_entry['defence2'], daily_entry['goalie'], daily_entry['util'],]
            actualValue = get_lineup_value(db, lineup, daily_entry['gamePkStart'], daily_entry['gamePkEnd'])

            logging.debug("Total actual value " + str(actualValue))
            logging.debug(lineup)
            if actualValue > 0:
                db.query('''UPDATE OR IGNORE daily_draftkings_entries
                             set actualValue = ?,
                                 updatedOn = ?
                             where id = ?''', (actualValue, datetime.datetime.today(), daily_entry['id'],))
            else:
                logging.error("Calculation for total value was 0, error in calculation or game hasn't been completed.")

    except Exception as e:
        logging.error("Could not update actual value for lineups for DraftKings:")
        logging.error("Got the following error:")
        logging.error(e)
        raise e

def calculate_statistics(db):

    logging.info("Updating actual value for player lineups for DraftKings....")
    try:
        update_lineup_values(db)
        update_entry_values(db)
        # find_optimal_lineups(c)

    except Exception as e:
        logging.error("Could not update actual value for lineups for DraftKings:")
        logging.error("Got the following error:")
        logging.error(e)
        db.rollback()
        exit()
