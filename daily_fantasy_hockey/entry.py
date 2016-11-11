import datetime
import logging

logger = logging.getLogger(__name__)

class Entry():
    _db = None
    _id = None
    _entryId = None
    _contestId = None
    _contestName = None
    _entryFee = None
    _lineup = None

    def __init__(self, db, entryId, contestName, contestId, entryFee, id = None, lineup = None):
        logging.debug("Initializing entry")
        self._db = db
        self._entryId = entryId
        self._contestId = contestId
        self._contestName = contestName
        self._entryFee = entryFee
        if id:
            self._id = id
        if lineup:
            self.set_lineup(lineup)

    def get_entry_id(self):
        return self._entryId

    def get_list(self):
        entry_info = []
        entry_info.append(self._entryId)
        entry_info.append(self._contestName)
        entry_info.append(self._contestId)
        entry_info.append(self._entryFee)
        if self._lineup:
            for info in self._lineup.get_list():
                entry_info.append(info)
        return entry_info

    def set_lineup(self, lineup):
        self._lineup = lineup
        self.update_entry()

    def insert_entry(self):
        logging.debug("Inserting entry for DraftKings")
        try:

            # Insert the entry (may not have the lineup yet)
            if self._lineup == None:
                entry_list = [self._entryId,
                              self._contestId,
                              self._contestName,
                              self._entryFee,
                              datetime.datetime.today(),
                              datetime.datetime.today()]
                # logging.debug(all_entrys_list)
                self._db.query('''INSERT OR IGNORE INTO daily_draftkings_entries
                             (entryId,
                              contestId,
                              contestName,
                              entryFee,
                              createdOn,
                              updatedOn) VALUES(?,?,?,?,?,?)''', entry_list)
                self._id = self._db.get_last_row_id()
            else:
                entry_list = [self._entryId,
                              self._contestId,
                              self._contestName,
                              self._entryFee,
                              self._lineup.get_centre1().get_player_id(),
                              self._lineup.get_centre2().get_player_id(),
                              self._lineup.get_winger1().get_player_id(),
                              self._lineup.get_winger2().get_player_id(),
                              self._lineup.get_winger3().get_player_id(),
                              self._lineup.get_defence1().get_player_id(),
                              self._lineup.get_defence2().get_player_id(),
                              self._lineup.get_goalie().get_player_id(),
                              self._lineup.get_util().get_player_id(),
                              self._lineup.get_total_weight(),
                              self._lineup.get_total_value(),
                              self._lineup.get_game_pk_start(),
                              self._lineup.get_game_pk_end(),
                              datetime.datetime.today(),
                              datetime.datetime.today()]
                # logging.debug(all_entrys_list)
                self._db.query('''INSERT OR IGNORE INTO daily_draftkings_entries
                             (entryId,
                              contestId,
                              contestName,
                              entryFee,
                              centre1,
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
                              updatedOn) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', entry_list)
                self._id = self._db.get_last_row_id()

        except Exception as e:
            logging.error("Could not insert entries for DraftKings:")
            logging.error("Got the following error:")
            logging.error(e)


            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e


    def update_entry(self):
        logging.debug("Update entry for DraftKings")
        try:

            # Update the entry (may not have the lineup yet)
            entry_list = [self._id,
                          self._entryId,
                          self._contestId,
                          self._contestName,
                          self._entryFee,
                          self._lineup.get_centre1().get_player_id(),
                          self._lineup.get_centre2().get_player_id(),
                          self._lineup.get_winger1().get_player_id(),
                          self._lineup.get_winger2().get_player_id(),
                          self._lineup.get_winger3().get_player_id(),
                          self._lineup.get_defence1().get_player_id(),
                          self._lineup.get_defence2().get_player_id(),
                          self._lineup.get_goalie().get_player_id(),
                          self._lineup.get_util().get_player_id(),
                          self._lineup.get_total_weight(),
                          self._lineup.get_total_value(),
                          self._lineup.get_game_pk_start(),
                          self._lineup.get_game_pk_end(),
                          datetime.datetime.today(),
                          datetime.datetime.today()]
            self._db.query('''INSERT OR REPLACE INTO daily_draftkings_entries
                         (id,
                          entryId,
                          contestId,
                          contestName,
                          entryFee,
                          centre1,
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
                          updatedOn) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', entry_list)

        except Exception as e:
            logging.error("Could not insert entries for DraftKings:")
            logging.error("Got the following error:")
            logging.error(e)


        # Roll back any change if something goes wrong
        # db.rollback()
        # raise e