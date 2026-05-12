# Esports Tournament System - ETL Pipeline

## Project Overview and Purpose

This project implements a complete Extract-Transform-Load (ETL) pipeline for esports tournament data. The system demonstrates a production-grade data processing workflow that ingests tournament, team, and match data from real and simulated sources, validates and transforms the data, and loads it into multiple output formats for analysis and reporting.

## Problem Being Addressed

Esports tournament data is fragmented across multiple sources with inconsistent formats and structures. This project provides a unified pipeline to:
- Consolidate data from heterogeneous sources (APIs and files)
- Validate data integrity and consistency
- Transform raw data into a normalized relational schema
- Generate multiple output formats (CSV, JSON, SQLite) for different use cases
- Provide sample queries and analytics on tournament data

## Data Sources

The pipeline uses a **hybrid data source model**:
- **Real Data**: Team and league information from the OpenDota API (Dota 2 professional esports)
- **Simulated Data**: Realistic match records are generated based on extracted teams, ensuring referential integrity while demonstrating the pipeline's ability to handle complex data relationships

## High-Level Pipeline Description

The ETL pipeline processes esports tournament data through three main stages:

1. **Extract**: Fetch team/league data from OpenDota API and generate realistic match scenarios
2. **Transform**: Validate, normalize, and derive data structures with comprehensive error handling
3. **Load**: Export to multiple formats (CSV, JSON) and populate SQLite database with full schema and foreign key constraints

The pipeline produces multiple outputs including flat files for easy access, a relational database for complex queries, and sample query results for immediate insights.

## Data Processing

**Extract:** 
- Fetches real team/league data from OpenDota API via HTTP
- Generates realistic match data based on extracted teams
- Handles API errors and network failures gracefully

**Transform:** 
- Validates required fields and data types
- Checks referential integrity between entities
- Derives game entities from league data
- Maps country codes to standardized regions
- Tracks all validation errors with specific context
- Outputs transformation statistics and error logs

**Load:** 
- Exports transformed data to CSV format in `transformed_data/` directory
- Exports transformed data to JSON format in `transformed_data/` directory
- Creates SQLite database (`tournament_system.db`) with complete relational schema
- Populates database tables with foreign key constraints enforced
- Executes sample queries and exports results to `sample_output.txt`

## Schema Design

**Games Table:**
- GameId (INTEGER PRIMARY KEY)
- Name (TEXT NOT NULL UNIQUE)
- Genre (TEXT)
- MaxTeamSize (INTEGER)

**Teams Table:**
- TeamId (INTEGER PRIMARY KEY)
- Name (TEXT NOT NULL)
- Region (TEXT)
- EstablishedDate (TEXT)

**Players Table:**
- PlayerId (INTEGER PRIMARY KEY)
- Name (TEXT NOT NULL)
- Region (TEXT)
- JoinDate (TEXT)

**TeamRosters Table:**
- RosterId (INTEGER PRIMARY KEY)
- TeamId (INTEGER NOT NULL, FOREIGN KEY → Teams.TeamId)
- PlayerId (INTEGER NOT NULL, FOREIGN KEY → Players.PlayerId)
- JoinDate (TEXT)
- Position (TEXT)

**Tournaments Table:**
- TournamentId (INTEGER PRIMARY KEY)
- Name (TEXT NOT NULL)
- GameId (INTEGER NOT NULL, FOREIGN KEY → Games.GameId)
- StartDate (TEXT)
- EndDate (TEXT)
- PrizePool (REAL)

**Matches Table:**
- MatchId (INTEGER PRIMARY KEY)
- TournamentId (INTEGER NOT NULL, FOREIGN KEY → Tournaments.TournamentId)
- Team1Id (INTEGER NOT NULL, FOREIGN KEY → Teams.TeamId)
- Team2Id (INTEGER NOT NULL, FOREIGN KEY → Teams.TeamId)
- WinnerTeamId (INTEGER NOT NULL, FOREIGN KEY → Teams.TeamId)
- MatchDate (TEXT)
- MapOrGame (TEXT)

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

## Expected Output

After running the pipeline, the following files are created:

### Output Files and Directories
- `transformed_data/` - Directory containing exported flat files
  - `teams.csv` - Team data in CSV format
  - `teams.json` - Team data in JSON format
  - `matches.csv` - Match data in CSV format
  - `matches.json` - Match data in JSON format
- `tournament_system.db` - SQLite database with complete schema and data
- `sample_output.txt` - Sample query results from the database

## Description of Outputs and Reports

### CSV Exports (`teams.csv`, `matches.csv`)
Comma-separated values format for easy import into spreadsheet applications and other data tools.

### JSON Exports (`teams.json`, `matches.json`)
JSON formatted data for use in web applications and APIs.

### SQLite Database (`tournament_system.db`)
Relational database containing all transformed data with:
- Foreign key constraints for referential integrity
- Normalized schema to eliminate data redundancy
- Support for complex queries and aggregations

### Sample Query Results (`sample_output.txt`)
Includes:
- Query 1: All games in the system
- Query 2: Teams aggregated by region
- Query 3: Match results with team names (multi-table JOIN)
- Query 4: Team rosters with player positions
- Query 5: Tournament summaries with prize pools
- Query 6: Match counts per tournament
- Query 7: Players by region and join date

The report demonstrates database capabilities including aggregations, joins, and filtering.

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

## Project Features
- Multi-source data ingestion (API + CSV)
- Comprehensive validation and error handling
- Clear ETL phase separation
- Proper relational schema design with FK constraints
- Multiple implementation languages
- Query capability and analytical output
- Full reproducibility with documentation
- Production-ready error tracking and logging

# AI Disclosure

The templating and structure of this README and sourcing datasets was generated via LLM assistance, but tweaked and with content written by myself.