# Capstone Milestone 2: ETL Prototype

## Overview

Full extract-transform-load pipeline using a raw esports data csv dump from OpenDota API

## How to Run

`ash
go run etl_pipeline.go
`

Output files generated in transformed_data/:
- teams.csv - 986 teams (normalized)
- teams.json - Same data in JSON
- matches.csv - 48 matches (validated)
- matches.json - Same data in JSON

## Data processing

**Extract:** Pulls real data from OpenDota API such as leagues or teams
**Transform:** Validates fields, joins references, reshapes timestamps and maps regions
**Load:** Exports to CSV/JSON
**Statistics:** 1048 records processed, 14 validation errors caught, 2 matches rejected

## Data structures used

- **Slices** - Dynamic arrays for record collections
- **Maps** - O(1) lookups for team validation
- **Structs** - Type-safe RawTeam, TransformedMatch, etc.
- **Functions** - Validation, transformation, region mapping

## Schema

Output directly maps to capstone design:
- teams.csv maps to Teams table (TeamId, Name, Tag, Region, EstablishedDate)
- matches.csv maps to Matches table (MatchId, LeagueId, Team1Id, Team2Id, WinnerTeamId, MatchDate, MapOrGame)
- Games derived from league data

## Potential improvements

- Load CSV into SQLite database
- Implement sample queries from schema
- Add player/roster extraction
- Build tournament structure

## Data Source

Real data sourced from: https://api.opendota.com/api/teams
Additional reference: https://docs.opendota.com/

# AI disclosure

AI was used in scraping data dumps and highlighting candidates to use for the project as well as creating the basic templating of this README. All work is my own.
