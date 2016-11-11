import logging
from database import database

logger = logging.getLogger(__name__)

class Player():
    _playerId = None
    _fullName = None
    _currentTeamId = None
    _db = None

    def __init__(self, db, playerId):
        self._playerId = playerId
        self._db = db

        # Find player information from database
        self._db.query("select p.fullName, p.currentTeamId from players p where p.id = ?",(self._playerId,))
        for player in self._db.fetchall():
            self._fullName = player['fullName']
            self._currentTeamId = player['currentTeamId']

    def get_player_id(self):
        return self._playerId

    def get_full_name(self):
        return self._fullName

    def get_current_team_id(self):
        return self._currentTeamId

class Game():
    _gamePk = None
    _gameDate = None
    _statusCode = None
    _awayTeamId = None
    _homeTeamId = None
    _db = None

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

class PlayerGame():
    _player = None
    _playerId = None
    _game = None
    _gamePk = None
    _opponentId = None
    _db = None

    def __init__(self, db, playerId, gamePk):
        self._db = db
        self._playerId = playerId
        self._player = Player(db, self._playerId)
        self._gamePk = gamePk
        self._game = Game(db, self._gamePk)
        self._opponentId = self.get_opponent_id()

    def get_opponent_id(self):
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