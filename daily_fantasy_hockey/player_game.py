import logging
from update_game_info import update_player

logger = logging.getLogger(__name__)

class Player():

    def __init__(self, db, playerId):
        self._playerId = playerId
        self._db = db

        # Find player information from database
        self._db.query("select p.fullName, p.currentTeamId, p.primaryPositionAbbr from players p where p.id = ?",(self._playerId,))
        for player in self._db.fetchall():
            self._name = player['fullName']
            self._currentTeamId = player['currentTeamId']
            self._primaryPosition = player['primaryPositionAbbr']

    def get_player_id(self):
        return self._playerId

    def get_name(self):
        return self._name

    def get_current_team_id(self):
        return self._currentTeamId

    def get_primary_position(self):
        return self._primaryPosition

class Game():

    def __init__(self, db, gamePk):
        self._gamePk = gamePk
        self._db = db

        # Find player information from database
        self._db.query("select g.gameDate, g.statusCode, g.awayTeamId, g.homeTeamId from games g where g.gamePk = ?", (self._gamePk,))
        for player in self._db.fetchall():
            self._gameDate = player['gameDate']
            self._statusCode = player['statusCode']
            self._awayTeamId = player['awayTeamId']
            self._homeTeamId = player['homeTeamId']

    def get_game_pk(self):
        return self._gamePk

class PlayerGame(Player, Game):
    _opponentId = None

    def __init__(self, db, playerId):
        Player.__init__(self, db, playerId)
        Game.__init__(self, db, self.find_game_pk())
        self._opponentId = self.find_opponent_id()

    # The following two methods are for printing and debugging
    def __str__(self):
        return self._nameAndId

    def __repr__(self):
        return self._nameAndId

    # Used to make the class subscriptable
    def __getitem__(self, item):
        return getattr(self, "_" + item)

    def get_opponent_id(self):
        return self._opponentId

    def find_game_pk(self):  # , date_for_lineup):
        try:
            logging.debug(
                "Getting player gamePk for player ID: " + str(self._playerId))  # + " on " + str(date_for_lineup))
            # Check for games that aren't finished (statusCode == 7), as future games aren't loaded in
            self._db.query('''select g.gamePk
                           from players p
                          inner join games g
                             on (p.currentTeamId = g.awayTeamId or
                                 p.currentTeamId = g.homeTeamId)
                          where p.id = ? and
                                g.statusCode != 7

                            union all

                         select g.gamePk
                           from players p
                          inner join games g
                             on (p.currentTeamId = g.awayTeamId or
                                 p.currentTeamId = g.homeTeamId)
                          where p.id = ? and
                                g.statusCode != 7''',
                           (self._playerId, self._playerId,))  # , date_for_lineup, player_id, date_for_lineup,))
            for player in self._db.fetchall():
                return player['gamePk']

            # Try updating player and trying again
            if self._playerId != None:
                logging.info("Could not find any gamePk for " + str(self._playerId) + ", trying again.")
                update_player(self._db, self._playerId, True)
                self._db.query('''select g.gamePk
                               from players p
                              inner join games g
                                 on (p.currentTeamId = g.awayTeamId or
                                     p.currentTeamId = g.homeTeamId)
                              where p.id = ? and
                                    g.statusCode != 7

                                union all

                             select g.gamePk
                               from players p
                              inner join games g
                                 on (p.currentTeamId = g.awayTeamId or
                                     p.currentTeamId = g.homeTeamId)
                              where p.id = ? and
                                    g.statusCode != 7''',
                               (self._playerId, self._playerId,))  # , date_for_lineup, player_id, date_for_lineup,))
                for player in self._db.fetchall():
                    return player['gamePk']

                    # raise ValueError("Could not find any gamePk for " + str(self._playerId))

        except Exception as e:
            logging.error("Could not find gamePk for " + str(self._playerId))
            logging.error("Got the following error:")
            logging.error(e)
            # raise e

    def find_opponent_id(self):
        try:
            logging.debug("Getting player opponent for player ID " + str(self._playerId) + " and gamePk " + str(self._gamePk))
            self._db.query('''select p.currentTeamId, g.homeTeamId, g.awayTeamId
                           from players p
                          inner join games g
                             on (p.currentTeamId = g.awayTeamId or
                                 p.currentTeamId = g.homeTeamId)
                          where p.id = ? and
                                g.gamePk = ?

                            union all

                         select p.currentTeamId, g.homeTeamId, g.awayTeamId
                           from players p
                          inner join games g
                             on (p.currentTeamId = g.awayTeamId or
                                 p.currentTeamId = g.homeTeamId)
                          where p.id = ? and
                                g.gamePk = ?''', (self._playerId, self._gamePk, self._playerId, self._gamePk,))

            for player in self._db.fetchall():
                if player['currentTeamId'] == player['homeTeamId']:
                    return player['awayTeamId']
                else:
                    return player['homeTeamId']

        except Exception as e:
            logging.error("Could not find player opponent for player ID " + str(self._playerId) + " and gamePk " + str(self._gamePk))
            logging.error("Got the following error:")
            logging.error(e)
            raise e