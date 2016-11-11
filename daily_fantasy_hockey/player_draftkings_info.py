import logging
from player import PlayerGame
from update_game_info import update_player

logger = logging.getLogger(__name__)


class PlayerDraftKingsInfo():
    _playerGame = None
    _name = None
    _draftKingsId = None
    _nameAndId = None
    _playerId = None
    _position = None
    _opponentId = None
    _gamePk = None
    _dateForLineup = None
    _gameInfo = None
    _teamAbbrev = None
    _draftType = None
    _value = None
    _weight = None
    _db = None

    def __init__(self, db, nameAndId, name=None, draftKingsId=None, weight=None, position=None,
                 gameInfo=None, teamAbbrev=None, draftType=None, dateForLineup=None):
        self._db = db

        # Find player ID from player name from draftkings (or if nameAndId, find from database)
        if name != None:
            self._draftKingsId = draftKingsId
            self._name = name
            self._nameAndId = self._name + " (" + str(self._draftKingsId) + ")"
            self._weight = weight
            self._position = "W" if position in ["LW", "RW"] else position
            self._gameInfo = gameInfo
            self._teamAbbrev = teamAbbrev
            self._draftType = draftType
            self._playerId = self.get_player_id_by_name()
            self._gamePk = self.get_game_pk()
            self._dateForLineup = dateForLineup
            self._value = self.calculate_value()
        else:
            self._nameAndId = nameAndId
            self.find_player_draftkings_info(db)

        # Initialize playerGame object
        self._playerGame = PlayerGame(db, self._playerId, self._gamePk)

        # Find information on player
        self._opponentId = self._playerGame.get_opponent_id()

    def find_player_draftkings_info(self, db):
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
                    where nameAndId = ?''', (self._nameAndId,))
        for player_info in db.fetchall():
            self._draftKingsId = player_info['id']
            self._name = player_info['name']
            self._weight = player_info['weight']
            self._position = player_info['position']
            self._gameInfo = player_info['gameInfo']
            self._teamAbbrev = player_info['teamAbbrev']
            self._draftType = player_info['draftType']
            self._playerId = player_info['playerId']
            self._gamePk = player_info['gamePk']
            self._dateForLineup = player_info['dateForLineup']
            self._value = player_info['value']

    def get_player_id_by_name(self):
        try:
            self._db.query("select p.id from players p where p.fullName = ?", (self._name,))
            for player in self._db.fetchall():
                return player['id']

        except Exception as e:
            logging.error("Could not find player ID for " + self._name)
            logging.error("Got the following error:")
            logging.error(e)
            raise e

    def get_player_id(self):
        return self._playerId

    def get_name_and_id(self):
        return self._nameAndId

    def get_position(self):
        return self._position

    def get_value(self):
        return self._value

    def add_value(self, value_to_add):
        self._value += value_to_add
        return self._value

    def get_game_pk(self):  # , date_for_lineup):
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
                logging.debug("Could not find any gamePk for " + str(self._playerId) + ", trying again.")
                update_player(self._db, self._playerId)
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
            logging.error("Could not find gamePk for " + str(self._name))
            logging.error("Got the following error:")
            logging.error(e)
            # raise e

    #
    # def get_player_info_by_name_and_draftkings_id(self):
    #     try:
    #         self._db.query("select pdi.* from player_draftkings_info pdi where pdi.nameAndId = ?", (self._nameAndId,))
    #         for player in self._db.fetchall():
    #             return player
    #
    #     except Exception as e:
    #         logging.error("Could not find player ID for " + self._nameAndId)
    #         logging.error("Got the following error:")
    #         logging.error(e)
    #         raise e

    def calculate_value(self):
        try:
            logging.debug("Getting player value for " + str(self._name))
            # Find average points for last week and for the year
            self._db.query('''select p.fullName,
                                ifnull(avg(case when g.gamePk like '2015%' then gdp.points else null end),0) AS average_points_last_year,
                                count(case when g.gamePk like '2015%' then gdp.points else null end) as games_last_year,
                                ifnull(avg(case when g.gamePk like '2016%' then gdp.points else null end),0) AS average_points_this_year,
                                count(case when g.gamePk like '2016%' then gdp.points else null end) as games_this_year,
                                ifnull(avg(case when g.gameDate > date('now','-14 day') then gdp.points else null end),0) AS average_points_last_two_weeks,
                                count(case when g.gameDate > date('now','-14 day') then gdp.points else null end) AS games_last_two_weeks
                         from games_draftkings_points gdp
                         inner join players p
                         on gdp.playerid = p.id
                         inner join games g
                         on gdp.gamePk = g.gamePk
                         where (g.gamePk like '2016%' or g.gamePk like '2015%') and
                               p.id = ?
                         group by p.id''', (self._playerId,))
            value = 0
            for player_stats in self._db.fetchall():
                # Calculate total games (will be over one due to last two weeks, but want to find the ratio for each stat)
                total_games = player_stats['games_last_year'] + player_stats['games_this_year'] + player_stats['games_last_two_weeks']
                logging.debug("Total games last year is " + str(player_stats['games_last_year']))
                logging.debug("Total games this year is " + str(player_stats['games_this_year']))
                logging.debug("Total games last two weeks is " + str(player_stats['games_last_two_weeks']))
                logging.debug("Average points last year is " + str(player_stats['average_points_last_year']))
                logging.debug("Average points this year is " + str(player_stats['average_points_this_year']))
                logging.debug("Average points last two weeks is " + str(player_stats['average_points_last_two_weeks']))

                # Calculate value (ignore players that haven't played a game this year)
                if player_stats['games_this_year'] != 0:
                    value = (player_stats['games_last_year'] / total_games) * player_stats[
                        'average_points_last_year'] + (player_stats['games_this_year'] / total_games) * player_stats[
                        'average_points_this_year'] + (player_stats['games_last_two_weeks'] / total_games) * player_stats[
                        'average_points_last_two_weeks']
                else:
                    return 0
                logging.debug("Value of " + str(value) + " for " + player_stats['fullName'])

            # Adjust value based on opponent
            # Get average goals against
            goals_against_average = 0
            self._db.query("select avg(ts.goalsAgainstPerGame) goals_against_percentage_average from team_stats ts")
            for player_stats in self._db.fetchall():
                goals_against_average = player_stats['goals_against_percentage_average']
            logging.debug("Goals against average across league: " + str(goals_against_average))

            # Get opponent goals against
            self._db.query('''select ts.goalsAgainstPerGame
                         from team_stats ts
                         where ts.teamId = ?''', (self._opponentId,))

            for player_stats in self._db.fetchall():
                # Calculate value
                logging.debug("Current value: " + str(value))
                goals_against_percentage = player_stats['goalsAgainstPerGame'] / goals_against_average
                value *= goals_against_percentage
                logging.debug("Got value of " + str(goals_against_percentage) + ", changing value to: " + str(value))

            return value

        except Exception as e:
            logging.error("Could not find player points for DraftKings:")
            logging.error("Got the following error:")
            logging.error(e)
            raise e

    def insert_player_data(self):
        logging.debug("Inserting player information for DraftKings for " + str(self._name))
        try:
            player_info_list = [self._draftKingsId,
                                self._name,
                                self._nameAndId,
                                self._playerId,
                                self._weight,
                                self._value,
                                self._position,
                                self._gameInfo,
                                self._opponentId,
                                self._gamePk,
                                self._teamAbbrev,
                                self._draftType,
                                self._dateForLineup]

            self._db.query('''INSERT OR IGNORE INTO player_draftkings_info
                    (id,
                     name,
                     nameAndId,
                     playerId,
                     weight,
                     value,
                     position,
                     gameInfo,
                     opponentId,
                     gamePk,
                     teamAbbrev,
                     draftType,
                     dateForLineup) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''', player_info_list)

        except Exception as e:
            logging.error("Could not insert player info for DraftKings.")
            logging.error("Got the following error:")
            logging.error(e)
            # Roll back any change if something goes wrong
            # db.rollback()
            # raise e

    def get_weight(self):
        return self._weight
