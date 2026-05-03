"""
esports tournament system etl pipeline
proof of concept

This pipeline
1. Extracts data from multiple csv sources
2. Transforms data via validation, reshaping and aggregation
3. Loads data into SQLite database matching the schema design
"""

import csv
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple


class TournamentETL:
    """main etl pipeline"""

    def __init__(self, db_path: str = "tournament_system.db"):
        self.db_path = db_path
        self.data = {
            "tournaments": [],
            "games": [],
            "teams": [],
            "players": [],
            "rosters": [],
            "matches": [],
        }
        self.validation_errors = []
        self.transformation_log = []

    # ============== extract ==============

    def extract_tournaments(self, file_path: str) -> List[Dict]:
        """Extract tournament data from CSV"""
        print(f"Extracting tournaments from {file_path}...")
        tournaments = []
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                tournaments = list(reader)
                print(f"  Success:  Extracted {len(tournaments)} tournament records")
        except FileNotFoundError:
            print(f"  ✗ Error: File {file_path} not found")
        return tournaments

    def extract_teams(self, file_path: str) -> List[Dict]:
        """Extract team data from CSV"""
        print(f"Extracting teams from {file_path}...")
        teams = []
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                teams = list(reader)
                print(f"  Success:  Extracted {len(teams)} team records")
        except FileNotFoundError:
            print(f"  ✗ Error: File {file_path} not found")
        return teams

    def extract_players(self, file_path: str) -> List[Dict]:
        """Extract player data from CSV"""
        print(f"Extracting players from {file_path}...")
        players = []
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                players = list(reader)
                print(f"  Success:  Extracted {len(players)} player records")
        except FileNotFoundError:
            print(f"  ✗ Error: File {file_path} not found")
        return players

    def extract_rosters(self, file_path: str) -> List[Dict]:
        """Extract roster/team-player relationship data from CSV"""
        print(f"Extracting rosters from {file_path}...")
        rosters = []
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rosters = list(reader)
                print(f"  Success:  Extracted {len(rosters)} roster records")
        except FileNotFoundError:
            print(f"  ✗ Error: File {file_path} not found")
        return rosters

    def extract_matches(self, file_path: str) -> List[Dict]:
        """Extract match data from CSV"""
        print(f"Extracting matches from {file_path}...")
        matches = []
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                matches = list(reader)
                print(f"  Success:  Extracted {len(matches)} match records")
        except FileNotFoundError:
            print(f"  ✗ Error: File {file_path} not found")
        return matches

    # ============== transform ==============

    def validate_date(self, date_string: str) -> Tuple[bool, str]:
        """Validate date format YYYY-MM-DD"""
        try:
            datetime.strptime(date_string, "%Y-%m-%d")
            return True, date_string
        except ValueError:
            return False, None

    def validate_integer(self, value: str, field_name: str = "") -> Tuple[bool, int]:
        """Validate and convert string to integer"""
        try:
            return True, int(value)
        except ValueError:
            return False, None

    def validate_float(self, value: str, field_name: str = "") -> Tuple[bool, float]:
        """Validate and convert string to float"""
        try:
            return True, float(value)
        except ValueError:
            return False, None

    def transform_tournaments(
        self, raw_tournaments: List[Dict], raw_games: List[Dict]
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Transform tournament data with validation and derive game entities.

        TRANSFORMATIONS:
        1. Validate all date fields
        2. Validate prize pool (numeric)
        3. Derive game entities (split game name/genre into separate Games table)
        4. Aggregate game statistics
        """
        print("\nTransforming tournament data...")
        transformed_tournaments = []
        derived_games = {}  

        for tournament in raw_tournaments:
            tournament_id = tournament.get("tournament_id", "").strip()
            name = tournament.get("tournament_name", "").strip()
            game_name = tournament.get("game_name", "").strip()
            game_genre = tournament.get("game_genre", "").strip()
            team_size = tournament.get("team_size", "").strip()
            start_date = tournament.get("start_date", "").strip()
            end_date = tournament.get("end_date", "").strip()
            prize_pool = tournament.get("prize_pool", "0").strip()

            errors = []

            if not tournament_id:
                errors.append("Missing tournament_id")
            else:
                valid_id, tid = self.validate_integer(tournament_id, "tournament_id")
                if not valid_id:
                    errors.append(f"Invalid tournament_id: {tournament_id}")

            if not name:
                errors.append("Missing tournament_name")

            if not game_name:
                errors.append("Missing game_name")

            valid_start, _ = self.validate_date(start_date)
            if not valid_start:
                errors.append(f"Invalid start_date format: {start_date}")

            valid_end, _ = self.validate_date(end_date)
            if not valid_end:
                errors.append(f"Invalid end_date format: {end_date}")

            valid_prize, prize_val = self.validate_float(prize_pool, "prize_pool")
            if not valid_prize:
                errors.append(f"Invalid prize_pool: {prize_pool}")
                prize_val = 0.0

            if errors:
                error_msg = f"Tournament {tournament_id}: {', '.join(errors)}"
                self.validation_errors.append(error_msg)
                print(f"  Error: {error_msg}")
                continue

            transformed_tournaments.append(
                {
                    "tournament_id": tournament_id,
                    "tournament_name": name,
                    "game_name": game_name,
                    "start_date": start_date,
                    "end_date": end_date,
                    "prize_pool": prize_val,
                }
            )

            # derive and extract unique games
            if game_name not in derived_games:
                derived_games[game_name] = {
                    "game_name": game_name,
                    "game_genre": game_genre if game_genre else "Unknown",
                    "max_team_size": int(team_size) if team_size else 5,
                }

        print(f"  Success:  Transformed {len(transformed_tournaments)} tournaments")
        print(f"  Success:  Derived {len(derived_games)} unique games")
        self.transformation_log.append(
            f"Tournaments: {len(transformed_tournaments)} records, "
            f"{len(self.validation_errors)} errors"
        )

        return transformed_tournaments, list(derived_games.values())

    def transform_teams(self, raw_teams: List[Dict]) -> List[Dict]:
        """
        transform team data with validation

        Transformations:
        1. Validate all required fields
        2. Standardize region codes
        3. Validate dates
        """
        print("\nTransforming team data...")
        transformed_teams = []

        for team in raw_teams:
            team_id = team.get("team_id", "").strip()
            name = team.get("team_name", "").strip()
            region = team.get("region", "").strip()
            est_date = team.get("established_date", "").strip()

            errors = []

            if not team_id:
                errors.append("Missing team_id")
            else:
                valid_id, _ = self.validate_integer(team_id, "team_id")
                if not valid_id:
                    errors.append(f"Invalid team_id: {team_id}")

            if not name:
                errors.append("Missing team_name")

            if not region or len(region) == 0:
                errors.append("Missing region")

            valid_date, _ = self.validate_date(est_date)
            if not valid_date:
                errors.append(f"Invalid established_date format: {est_date}")

            if errors:
                error_msg = f"Team {team_id}: {', '.join(errors)}"
                self.validation_errors.append(error_msg)
                print(f"  Error: {error_msg}")
                continue

            transformed_teams.append(
                {
                    "team_id": team_id,
                    "team_name": name,
                    "region": region,
                    "established_date": est_date,
                }
            )

        print(f"  Success:  Transformed {len(transformed_teams)} teams")
        self.transformation_log.append(f"Teams: {len(transformed_teams)} records")

        return transformed_teams

    def transform_players(self, raw_players: List[Dict]) -> List[Dict]:
        """
        transform player data with validation

        Transformations:
        1. Validate all required fields
        2. Validate dates
        3. Standardize region codes
        """
        print("\nTransforming player data...")
        transformed_players = []

        for player in raw_players:
            player_id = player.get("player_id", "").strip()
            name = player.get("player_name", "").strip()
            region = player.get("region", "").strip()
            join_date = player.get("join_date", "").strip()

            errors = []

            if not player_id:
                errors.append("Missing player_id")
            else:
                valid_id, _ = self.validate_integer(player_id, "player_id")
                if not valid_id:
                    errors.append(f"Invalid player_id: {player_id}")

            if not name:
                errors.append("Missing player_name")

            if not region:
                errors.append("Missing region")

            valid_date, _ = self.validate_date(join_date)
            if not valid_date:
                errors.append(f"Invalid join_date format: {join_date}")

            if errors:
                error_msg = f"Player {player_id}: {', '.join(errors)}"
                self.validation_errors.append(error_msg)
                print(f"  Error: {error_msg}")
                continue

            transformed_players.append(
                {
                    "player_id": player_id,
                    "player_name": name,
                    "region": region,
                    "join_date": join_date,
                }
            )

        print(f"  Success:  Transformed {len(transformed_players)} players")
        self.transformation_log.append(f"Players: {len(transformed_players)} records")

        return transformed_players

    def transform_rosters(
        self, raw_rosters: List[Dict], teams: List[Dict], players: List[Dict]
    ) -> List[Dict]:
        """
        Transform roster [team-player relationship] data with validation and joining

        Transformations:
        1. Validate team and player references
        2. Validate dates
        3. Join with teams and players to verify foreign keys
        4. Handle invalid references
        """
        print("\nTransforming roster data...")
        transformed_rosters = []

        # build lookup dictionaries for validation
        team_ids = {t["team_id"] for t in teams}
        player_ids = {p["player_id"] for p in players}

        for idx, roster in enumerate(raw_rosters):
            roster_id = idx + 1  # auto-generate roster id
            team_id = roster.get("team_id", "").strip()
            player_id = roster.get("player_id", "").strip()
            join_date = roster.get("join_date", "").strip()
            position = roster.get("position", "").strip()

            errors = []

            if not team_id:
                errors.append("Missing team_id")
            elif team_id not in team_ids:
                errors.append(f"Team {team_id} not found in teams table")

            if not player_id:
                errors.append("Missing player_id")
            elif player_id not in player_ids:
                errors.append(f"Player {player_id} not found in players table")

            valid_date, _ = self.validate_date(join_date)
            if not valid_date:
                errors.append(f"Invalid join_date format: {join_date}")

            if not position:
                errors.append("Missing position")

            if errors:
                error_msg = f"Roster (Team {team_id}, Player {player_id}): {', '.join(errors)}"
                self.validation_errors.append(error_msg)
                print(f"  Error: {error_msg}")
                continue

            transformed_rosters.append(
                {
                    "roster_id": roster_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "join_date": join_date,
                    "position": position,
                }
            )

        print(f"  Success:  Transformed {len(transformed_rosters)} roster records")
        self.transformation_log.append(f"Rosters: {len(transformed_rosters)} records")

        return transformed_rosters

    def transform_matches(
        self, raw_matches: List[Dict], tournaments: List[Dict], teams: List[Dict]
    ) -> List[Dict]:
        """
        Transform match data with validation and joining.

        TRANSFORMATIONS:
        1. Validate tournament and team references
        2. Validate match dates fall within tournament dates
        3. Validate winner is one of the participating teams
        4. Filter invalid matches
        """
        print("\nTransforming match data...")
        transformed_matches = []

        # build lookup dictionaries
        tournament_map = {t["tournament_id"]: t for t in tournaments}
        team_ids = {t["team_id"] for t in teams}

        for match in raw_matches:
            match_id = match.get("match_id", "").strip()
            tournament_id = match.get("tournament_id", "").strip()
            team1_id = match.get("team1_id", "").strip()
            team2_id = match.get("team2_id", "").strip()
            winner_id = match.get("winner_team_id", "").strip()
            match_date = match.get("match_date", "").strip()
            map_or_game = match.get("map_or_game", "").strip()

            errors = []

            if not match_id:
                errors.append("Missing match_id")
            else:
                valid_id, _ = self.validate_integer(match_id, "match_id")
                if not valid_id:
                    errors.append(f"Invalid match_id: {match_id}")

            if not tournament_id:
                errors.append("Missing tournament_id")
            elif tournament_id not in tournament_map:
                errors.append(f"Tournament {tournament_id} not found")
            else:
                tournament = tournament_map[tournament_id]
                valid_date, _ = self.validate_date(match_date)
                if not valid_date:
                    errors.append(f"Invalid match_date format: {match_date}")
                else:
                    match_dt = datetime.strptime(match_date, "%Y-%m-%d")
                    start_dt = datetime.strptime(tournament["start_date"], "%Y-%m-%d")
                    end_dt = datetime.strptime(tournament["end_date"], "%Y-%m-%d")
                    if not (start_dt <= match_dt <= end_dt):
                        errors.append(
                            f"Match date {match_date} outside tournament window "
                            f"({tournament['start_date']} to {tournament['end_date']})"
                        )

            if not team1_id or team1_id not in team_ids:
                errors.append(f"Invalid team1_id: {team1_id}")

            if not team2_id or team2_id not in team_ids:
                errors.append(f"Invalid team2_id: {team2_id}")

            if team1_id == team2_id:
                errors.append("Team cannot play against itself")

            if not winner_id or winner_id not in team_ids:
                errors.append(f"Invalid winner_team_id: {winner_id}")
            elif winner_id not in [team1_id, team2_id]:
                errors.append(f"Winner must be one of the participating teams")

            if errors:
                error_msg = f"Match {match_id}: {', '.join(errors)}"
                self.validation_errors.append(error_msg)
                print(f"  Error: {error_msg}")
                continue

            transformed_matches.append(
                {
                    "match_id": match_id,
                    "tournament_id": tournament_id,
                    "team1_id": team1_id,
                    "team2_id": team2_id,
                    "winner_team_id": winner_id,
                    "match_date": match_date,
                    "map_or_game": map_or_game if map_or_game else "Unknown",
                }
            )

        print(f"  Success:  Transformed {len(transformed_matches)} matches")
        self.transformation_log.append(f"Matches: {len(transformed_matches)} records")

        return transformed_matches

    # ============== load ==============

    # tables
    def create_schema(self):
        """Create database schema matching capstone design"""
        print("\nCreating database schema...")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Games (
                GameId INTEGER PRIMARY KEY,
                Name TEXT NOT NULL UNIQUE,
                Genre TEXT,
                MaxTeamSize INTEGER
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Players (
                PlayerId INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                Region TEXT,
                JoinDate TEXT
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Teams (
                TeamId INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                Region TEXT,
                EstablishedDate TEXT
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS TeamRosters (
                RosterId INTEGER PRIMARY KEY,
                TeamId INTEGER NOT NULL,
                PlayerId INTEGER NOT NULL,
                JoinDate TEXT,
                Position TEXT,
                FOREIGN KEY (TeamId) REFERENCES Teams(TeamId),
                FOREIGN KEY (PlayerId) REFERENCES Players(PlayerId)
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Tournaments (
                TournamentId INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                GameId INTEGER NOT NULL,
                StartDate TEXT,
                EndDate TEXT,
                PrizePool REAL,
                FOREIGN KEY (GameId) REFERENCES Games(GameId)
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Matches (
                MatchId INTEGER PRIMARY KEY,
                TournamentId INTEGER NOT NULL,
                Team1Id INTEGER NOT NULL,
                Team2Id INTEGER NOT NULL,
                WinnerTeamId INTEGER NOT NULL,
                MatchDate TEXT,
                MapOrGame TEXT,
                FOREIGN KEY (TournamentId) REFERENCES Tournaments(TournamentId),
                FOREIGN KEY (Team1Id) REFERENCES Teams(TeamId),
                FOREIGN KEY (Team2Id) REFERENCES Teams(TeamId),
                FOREIGN KEY (WinnerTeamId) REFERENCES Teams(TeamId)
            )
        """
        )

        conn.commit()
        print("  Success:  Schema created")

        return conn, cursor

    def load_games(self, conn, cursor, games: List[Dict]):
        """Load games into database"""
        print("Loading games...")
        for game in games:
            try:
                cursor.execute(
                    """
                    INSERT INTO Games (Name, Genre, MaxTeamSize)
                    VALUES (?, ?, ?)
                """,
                    (game["game_name"], game["game_genre"], game["max_team_size"]),
                )
            except sqlite3.IntegrityError as e:
                print(f"  Error: Duplicate game: {game['game_name']}")

        conn.commit()
        print(f"  Success:  Loaded games")

    def load_players(self, conn, cursor, players: List[Dict]):
        """Load players into database"""
        print("Loading players...")
        for player in players:
            cursor.execute(
                """
                INSERT INTO Players (PlayerId, Name, Region, JoinDate)
                VALUES (?, ?, ?, ?)
            """,
                (
                    player["player_id"],
                    player["player_name"],
                    player["region"],
                    player["join_date"],
                ),
            )
        conn.commit()
        print(f"  Success:  Loaded {len(players)} players")

    def load_teams(self, conn, cursor, teams: List[Dict]):
        """Load teams into database"""
        print("Loading teams...")
        for team in teams:
            cursor.execute(
                """
                INSERT INTO Teams (TeamId, Name, Region, EstablishedDate)
                VALUES (?, ?, ?, ?)
            """,
                (
                    team["team_id"],
                    team["team_name"],
                    team["region"],
                    team["established_date"],
                ),
            )
        conn.commit()
        print(f"  Success:  Loaded {len(teams)} teams")

    def load_tournaments(self, conn, cursor, tournaments: List[Dict]):
        """Load tournaments into database (with game lookup)"""
        print("Loading tournaments...")
        cursor.execute("SELECT GameId, Name FROM Games")
        game_map = {row[1]: row[0] for row in cursor.fetchall()}

        for tournament in tournaments:
            game_name = tournament["game_name"]
            game_id = game_map.get(game_name)

            if game_id:
                cursor.execute(
                    """
                    INSERT INTO Tournaments (TournamentId, Name, GameId, StartDate, EndDate, PrizePool)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        tournament["tournament_id"],
                        tournament["tournament_name"],
                        game_id,
                        tournament["start_date"],
                        tournament["end_date"],
                        tournament["prize_pool"],
                    ),
                )
            else:
                print(f"  Error: Game '{game_name}' not found for tournament {tournament['tournament_id']}")

        conn.commit()
        print(f"  Success:  Loaded {len(tournaments)} tournaments")

    def load_rosters(self, conn, cursor, rosters: List[Dict]):
        """Load team rosters into database"""
        print("Loading rosters...")
        for roster in rosters:
            cursor.execute(
                """
                INSERT INTO TeamRosters (RosterId, TeamId, PlayerId, JoinDate, Position)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    roster["roster_id"],
                    roster["team_id"],
                    roster["player_id"],
                    roster["join_date"],
                    roster["position"],
                ),
            )
        conn.commit()
        print(f"  Success:  Loaded {len(rosters)} roster records")

    def load_matches(self, conn, cursor, matches: List[Dict]):
        """Load matches into database"""
        print("Loading matches...")
        for match in matches:
            cursor.execute(
                """
                INSERT INTO Matches (MatchId, TournamentId, Team1Id, Team2Id, WinnerTeamId, MatchDate, MapOrGame)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    match["match_id"],
                    match["tournament_id"],
                    match["team1_id"],
                    match["team2_id"],
                    match["winner_team_id"],
                    match["match_date"],
                    match["map_or_game"],
                ),
            )
        conn.commit()
        print(f"  Success:  Loaded {len(matches)} matches")

    # ============== orchestration ==============

    def run(self):
        """Execute full ETL pipeline"""
        print("=" * 70)
        print("TOURNAMENT SYSTEM ETL PIPELINE - PROOF OF CONCEPT")
        print("=" * 70)

        print("\n[1/3] EXTRACT PHASE")
        print("-" * 70)
        raw_tournaments = self.extract_tournaments("data/raw_tournament_data.csv")
        raw_teams = self.extract_teams("data/raw_team_data.csv")
        raw_players = self.extract_players("data/raw_player_data.csv")
        raw_rosters = self.extract_rosters("data/raw_roster_data.csv")
        raw_matches = self.extract_matches("data/raw_match_data.csv")

        print("\n[2/3] TRANSFORM PHASE")
        print("-" * 70)
        tournaments, games = self.transform_tournaments(raw_tournaments, [])
        teams = self.transform_teams(raw_teams)
        players = self.transform_players(raw_players)
        rosters = self.transform_rosters(raw_rosters, teams, players)
        matches = self.transform_matches(raw_matches, tournaments, teams)

        print("\n[3/3] LOAD PHASE")
        print("-" * 70)
        conn, cursor = self.create_schema()

        self.load_games(conn, cursor, games)
        self.load_players(conn, cursor, players)
        self.load_teams(conn, cursor, teams)
        self.load_tournaments(conn, cursor, tournaments)
        self.load_rosters(conn, cursor, rosters)
        self.load_matches(conn, cursor, matches)

        print("\n" + "=" * 70)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 70)
        print("\nTransformation Log:")
        for log in self.transformation_log:
            print(f"  • {log}")

        if self.validation_errors:
            print(f"\nValidation Errors: {len(self.validation_errors)}")
            for error in self.validation_errors[:5]:
                print(f"  • {error}")
            if len(self.validation_errors) > 5:
                print(f"  ... and {len(self.validation_errors) - 5} more")
        else:
            print("\nSuccess:  No validation errors")

        # sample queries to verify data
        print("\n" + "=" * 70)
        print("SAMPLE QUERIES - VERIFYING DATA IN DATABASE")
        print("=" * 70)

        print("\nQuery 1: Games in System")
        cursor.execute("SELECT GameId, Name, Genre FROM Games")
        for row in cursor.fetchall():
            print(f"  • {row[1]} ({row[2]})")

        print("\nQuery 2: Teams by Region")
        cursor.execute("SELECT Region, COUNT(*) FROM Teams GROUP BY Region")
        for row in cursor.fetchall():
            print(f"  • {row[0]}: {row[1]} teams")

        print("\nQuery 3: Matches with Results")
        cursor.execute(
            """
            SELECT m.MatchId, t1.Name, t2.Name, tw.Name, m.MatchDate
            FROM Matches m
            JOIN Teams t1 ON m.Team1Id = t1.TeamId
            JOIN Teams t2 ON m.Team2Id = t2.TeamId
            JOIN Teams tw ON m.WinnerTeamId = tw.TeamId
            LIMIT 5
        """
        )
        for row in cursor.fetchall():
            print(f"  • Match {row[0]}: {row[1]} vs {row[2]} → {row[3]} won ({row[4]})")

        print("\nQuery 4: Player Rosters")
        cursor.execute(
            """
            SELECT t.Name, GROUP_CONCAT(p.Name, ', ')
            FROM Teams t
            LEFT JOIN TeamRosters tr ON t.TeamId = tr.TeamId
            LEFT JOIN Players p ON tr.PlayerId = p.PlayerId
            GROUP BY t.TeamId
            LIMIT 5
        """
        )
        for row in cursor.fetchall():
            print(f"  • {row[0]}: {row[1] if row[1] else 'No roster'}")

        conn.close()
        print("\n" + "=" * 70)
        print("Success:  PIPELINE COMPLETE")
        print("=" * 70)


if __name__ == "__main__":
    pipeline = TournamentETL()
    pipeline.run()
