import datetime
import logging
from player_draftkings_info import PlayerDraftKingsInfo

logger = logging.getLogger(__name__)

class Lineup():
    _db = None
    _players = None
    _dateForLineup = None
    _gamePkStart = None
    _gamePkEnd = None
    _totalValue = None
    _totalWeight = None

    def __init__(self, db, playerInfo):
        self._db = db
        self._players = []
        self._gamePkStart = 9999999999
        self._gamePkEnd = 0
        for i in range(9):
            player_info = PlayerDraftKingsInfo(db, playerInfo[i])
            self._players.append(player_info)

            gamePk = player_info.get_game_pk()
            if gamePk != None:
                if gamePk > self._gamePkEnd:
                    self._gamePkEnd = gamePk
                if gamePk < self._gamePkStart:
                    self._gamePkStart = gamePk

        self._totalValue = self.calculate_lineup_value()
        self._totalWeight = self.calculate_lineup_weight()

    def get_centre1(self):
        return self._players[0]

    def get_centre2(self):
        return self._players[1]

    def get_winger1(self):
        return self._players[2]

    def get_winger2(self):
        return self._players[3]

    def get_winger3(self):
        return self._players[4]

    def get_defence1(self):
        return self._players[5]

    def get_defence2(self):
        return self._players[6]

    def get_goalie(self):
        return self._players[7]

    def get_util(self):
        return self._players[8]

    def get_total_value(self):
        return self._totalValue

    def get_total_weight(self):
        return self._totalWeight

    def get_game_pk_start(self):
        return self._gamePkStart

    def get_game_pk_end(self):
        return self._gamePkEnd

    def get_list(self):
        lineup_info = []
        for player in self._players:
            lineup_info.append(player.get_name_and_id())
        lineup_info.append(self._totalValue)
        lineup_info.append(self._totalWeight)
        return lineup_info

    def calculate_lineup_value(self):
        logging.debug("Getting lineup value")
        logging.debug(self._players)
        try:
            total_value = 0
            for player in self._players:
                total_value += player.calculate_value()
            return total_value

        except Exception as e:
            logging.error("Could not get lineup value")
            logging.error(self._players)
            logging.error("Got the following error:")
            logging.error(e)
            raise e

    def calculate_lineup_weight(self):
        logging.debug("Getting lineup weight")
        logging.debug(self._players)
        try:
            total_weight = 0
            for player in self._players:
                total_weight += player.get_weight()
            return total_weight

        except Exception as e:
            logging.error("Could not get lineup weight")
            logging.error(self._players)
            logging.error("Got the following error:")
            logging.error(e)
            raise e

    def insert_lineup(self):
        logging.debug("Inserting lineups for DraftKings")
        logging.debug(self._players)

        try:
            # Get player information for each lineup
            # lineupWithPlayerInfo = []
            # for i in range(9):
            #     lineupWithPlayerInfo.append(get_player_info_by_name_and_draftkings_id(lineup[i]))

            # total_weight = lineup[9]
            # adjusted_total_value = lineup[10]
            # logging.debug(lineupWithPlayerInfo)

            # Get the actual total value, as the value in the lineup can be adjusted
            total_value = self.get_total_value()
            total_weight = self.get_total_weight()
            # logging.debug("Initial total value: " + str(total_value))
            # logging.debug("Adjusted total value: " + str(adjusted_total_value))

            # Insert the lineup
            lineup_list = [self._players[0].get_player_id(),
                           self._players[1].get_player_id(),
                           self._players[2].get_player_id(),
                           self._players[3].get_player_id(),
                           self._players[4].get_player_id(),
                           self._players[5].get_player_id(),
                           self._players[6].get_player_id(),
                           self._players[7].get_player_id(),
                           self._players[8].get_player_id(),
                           total_weight,
                           total_value,
                           self._gamePkStart,
                           self._gamePkEnd,
                           datetime.datetime.today(),
                           datetime.datetime.today()]
            # logging.debug(all_lineups_list)
            self._db.query('''INSERT OR IGNORE INTO daily_draftkings_lineups
                         (centre1,
                          centre2,
                          winger1,
                          winger2,
                          winger3,
                          defence1,
                          defence2,
                          goalie,
                          util,
                          totalWeight,
                          totalValue,
                          gamePkStart,
                          gamePkEnd,
                          createdOn,
                          updatedOn) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', lineup_list)

        except Exception as e:
            logging.error("Could not insert lineups for DraftKings:")
            logging.error(self._players)
            logging.error("Got the following error:")
            logging.error(e)


            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e
