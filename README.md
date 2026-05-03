# Capstone Milestone 2: ETL Prototype

## Overview

Full extract-transform-load pipeline using a raw esports data csv dump from OpenDota API

## How to Run
```
go run etl_pipeline.go
```

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



# Data Source

Real data sourced from: https://api.opendota.com/api/teams
Additional reference: https://docs.opendota.com/


## current working pipeline:
- extracts real esports data csv dumps from OpenDota API 
- validates and transforms records 
- exports normalized teams and matches to csv & json formats with statistics on processing results

## incomplete or simplified:
- region mapping logic and timestamp standardization could potentially be expanded to handle edge cases and provide additional data source formats for comprehensive coverage

## challenges i encountered:
- handling malformed api responses & ensuring referential integrity when matching teams across different data sources required careful validation and error handling strategies such as skipping invalid records while maintaining overall pipeline robustness

# next steps:
- load CSV into SQLite database
- implement sample queries from schema
- add player/roster extraction
- build tournament structure

# AI disclosure

AI was used in scraping data dumps and highlighting candidates to use for the project as well as creating the basic templating of this README. All work is my own.
