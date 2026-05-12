# Esports Tournament System - ETL Pipeline

## Overview

Complete extract-transform-load pipeline for esports tournament data. Go implementation that processes from OpenDota API, with comprehensive validation, CSV/JSON export, and SQLite database loading with sample query results.

## How to build and run:

> build
```
go build -o etl_pipeline.exe etl_pipeline.go

```
> run
```
go run etl_pipeline.go
.\etl_pipeline.exe
```

Output:
- `transformed_data/teams.csv`, `transformed_data/teams.json`, `transformed_data/matches.csv`, `transformed_data/matches.json`
- `tournament_system.db` (SQLite database with schema and loaded data)
- `sample_output.txt` (sample query results from the database)

## Data Processing

**Extract:** 
- Fetches real team/league data from OpenDota API via HTTP
- Generates realistic match data

**Transform:** 
- Validates required fields and data types
- Checks referential integrity
- Derives game entities from league data
- Maps country codes to regions
- Tracks validation errors with specific context

**Load:** 
- Exports to CSV and JSON in `transformed_data/`
- Creates SQLite database with full schema and foreign key constraints
- Populates all tables with transformed data
- Executes sample queries and exports results to text file

**Schema Design:**
- Games: GameId (PK), Name, Genre, MaxTeamSize
- Teams: TeamId (PK), Name, Region, EstablishedDate
- Players: PlayerId (PK), Name, Region, JoinDate
- Tournaments: TournamentId (PK), Name, GameId (FK), StartDate, EndDate, PrizePool
- TeamRosters: RosterId (PK), TeamId (FK), PlayerId (FK), JoinDate, Position
- Matches: MatchId (PK), TournamentId (FK), Team1Id (FK), Team2Id (FK), WinnerTeamId (FK), MatchDate, MapOrGame



## Data Sources

- **OpenDota API**: https://api.opendota.com/api/teams and /api/leagues (Go pipeline)
- **CSV Files**: Tournament, team, player, roster, and match data (Python pipeline)
- **Volume**: 1000+ teams, 9725+ leagues extracted; 5 tournaments, 14 players, 50 matches processed

## Sample Output

**Execution Statistics:**
- Teams: 986 extracted, 986 transformed (14 validation errors)
- Matches: 50 generated, 48 transformed (2 validation errors)
- Games: 1 derived
- Tournaments: 5 loaded
- Players: 14 loaded
- Rosters: 13 team-player assignments loaded

**Query Results** (see `sample_output.txt` for full output):
- Query 1: Games in System
- Query 2: Teams by Region (aggregation)
- Query 3: Match Results with Team Names (multi-table JOIN)
- Query 4: Team Rosters with Player Positions (nested JOIN)
- Query 5: Tournament Summary with Prize Pools
- Query 6: Match Count per Tournament
- Query 7: Players by Region and Join Date

## Key Technical Decisions

**Error Handling Strategy:**
- Validates early and logs errors, but continues processing remaining records
- Prevents single bad records from blocking entire pipeline
- All validation failures tracked with specific error context

**Foreign Key Validation:**
- Python pipeline builds lookup maps during transform phase before insert
- Catches referential violations before database constraints trigger
- Prevents transaction rollbacks with diagnostic error messages

**Region Mapping:**
- Centralized `mapCountryToRegion()` function provides single source of truth
- Standardizes country code to region transformation (ISO 3166-1 → tournament region)

**Multiple Output Formats:**
- CSV: Human-readable, Excel-compatible for reporting
- JSON: API-ready nested format
- SQLite: Relational queries with enforced constraints

## Storage Design Rationale

**Why SQLite:**
- Serverless deployment (no external database infrastructure)
- Full ACID compliance with foreign key support
- Suitable for capstone scope (< 100k records)
- Simple backup and version control

**Why Dual Implementations:**
- Go: Real-time API data ingestion, systems-level programming
- Python: Rapid development, production data loads, idiomatic approach
- Both validate identical transformations; interchangeable inputs/outputs

## Assumptions & Constraints

**Assumptions:**
- Tournament start_date < end_date
- Teams and players have globally unique IDs
- Match dates fall within tournament windows
- Teams cannot play themselves
- Country codes follow ISO 3166-1 alpha-2 standard

**Constraints:**
- Single-threaded; no parallel processing
- Batch processing only (no real-time streaming)
- SQLite suitable for < 100k records
- Hard-coded region mappings
- Manual pipeline execution (no scheduling)

## Project Structure

```
.
├── etl_pipeline.go               # Go: API extraction + CSV/JSON export
├── etl_pipeline.py               # Python: CSV extraction + SQLite load
├── go.mod / go.sum              # Go module dependencies
├── README.md                     # This file
├── data/
│   ├── raw_tournament_data.csv
│   ├── raw_team_data.csv
│   ├── raw_player_data.csv
│   ├── raw_roster_data.csv
│   └── raw_match_data.csv
├── transformed_data/
│   ├── teams.csv / teams.json
│   └── matches.csv / matches.json
├── tournament_system.db          # SQLite database
└── sample_output.txt             # Query results export
```

## What This Project Demonstrates

- Multi-source data ingestion (API + CSV)
- Comprehensive validation and error handling
- Clear ETL phase separation
- Proper relational schema design with FK constraints
- Multiple implementation languages
- Query capability and analytical output
- Full reproducibility with documentation
- Production-ready error tracking and logging
