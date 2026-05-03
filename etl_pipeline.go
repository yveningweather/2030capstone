package main

import (
    "encoding/csv"
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

// ============== data structures ==============

type RawTeam struct {
    TeamID   int    `json:"team_id"`
    Name     string `json:"name"`
    Tag      string `json:"tag"`
    LogoURL  string `json:"logo_url"`
    Country  string `json:"country_code"`
}

type RawMatch struct {
    MatchID           int64  `json:"match_id"`
    RadiantTeamID     int    `json:"radiant_team_id"`
    DireTeamID        int    `json:"dire_team_id"`
    RadiantWin        bool   `json:"radiant_win"`
    StartTime         int64  `json:"start_time"`
    Duration          int    `json:"duration"`
    SeriesType        int    `json:"series_type"`
    SeriesID          int    `json:"series_id"`
    LeagueID          int    `json:"league_id"`
    Season            int    `json:"season"`
    Radiant           RawTeamInfo `json:"radiant"`
    Dire              RawTeamInfo `json:"dire"`
}

type RawTeamInfo struct {
    TeamID int    `json:"team_id"`
    Name   string `json:"name"`
    Tag    string `json:"tag"`
}

type RawLeague struct {
    LeagueID    int    `json:"league_id"`
    Name        string `json:"name"`
    Description string `json:"description"`
    DisplayName string `json:"display_name"`
    Tier        string `json:"tier"`
    Image       string `json:"image_url"`
}

// ============== transformed data ==============

type TransformedGame struct {
    GameID      int
    Name        string
    Genre       string
    MaxTeamSize int
}

type TransformedTeam struct {
    TeamID        int
    Name          string
    Tag           string
    Region        string
    EstablishedDate string
}

type TransformedMatch struct {
    MatchID       int64
    LeagueID      int
    Team1ID       int
    Team2ID       int
    WinnerTeamID  int
    MatchDate     string
    Duration      int
    MapOrGame     string
}

// ============== extractor ==============

type ETLPipeline struct {
    Teams               []RawTeam
    Matches             []RawMatch
    Leagues             []RawLeague
    ValidationErrors    []string
    TransformationLog   []string
}

func NewETLPipeline() *ETLPipeline {
    return &ETLPipeline{
        Teams:            []RawTeam{},
        Matches:          []RawMatch{},
        Leagues:          []RawLeague{},
        ValidationErrors: []string{},
        TransformationLog: []string{},
    }
}

func (e *ETLPipeline) ExtractTeams() error {
    fmt.Println("Extracting teams from OpenDota API...")

    resp, err := http.Get("https://api.opendota.com/api/teams")
    if err != nil {
        return fmt.Errorf("failed to fetch teams: %w", err)
    }
    defer resp.Body.Close()

    if err := json.NewDecoder(resp.Body).Decode(&e.Teams); err != nil {
        return fmt.Errorf("failed to decode teams: %w", err)
    }

    fmt.Printf("  Success:  Extracted %d teams\n", len(e.Teams))
    return nil
}

func (e *ETLPipeline) ExtractMatches(limit int) error {
    fmt.Printf("Generating matches from realistic tournament data (limit: %d)...\n", limit)

    if len(e.Teams) < 2 {
        return fmt.Errorf("not enough teams extracted to generate matches")
    }

    baseTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC).Unix()

    for i := 0; i < limit && i < len(e.Teams)-1; i++ {
        team1ID := e.Teams[i].TeamID
        team2ID := e.Teams[i+1].TeamID

        winner := team1ID
        if i%3 == 0 {
            winner = team2ID
        }

        match := RawMatch{
            MatchID:       int64(7893247500 + i),
            RadiantTeamID: team1ID,
            DireTeamID:    team2ID,
            RadiantWin:    team1ID == winner,
            StartTime:     baseTime + int64(i*86400),
            Duration:      1800 + (i*60)%3600,
            SeriesType:    3,
            SeriesID:      1000 + int(int64(i)/5),
            LeagueID:      14983,
            Season:        2024,
        }

        e.Matches = append(e.Matches, match)
    }

    fmt.Printf("  Success:  Generated %d realistic matches\n", len(e.Matches))
    return nil
}

func (e *ETLPipeline) ExtractLeagues() error {
    fmt.Println("Extracting leagues from OpenDota API...")

    resp, err := http.Get("https://api.opendota.com/api/leagues")
    if err != nil {
        return fmt.Errorf("failed to fetch leagues: %w", err)
    }
    defer resp.Body.Close()

    if err := json.NewDecoder(resp.Body).Decode(&e.Leagues); err != nil {
        return fmt.Errorf("failed to decode leagues: %w", err)
    }

    fmt.Printf("  Success:  Extracted %d leagues\n", len(e.Leagues))
    return nil
}

// ============== transformer ==============

func (e *ETLPipeline) TransformTeams() []TransformedTeam {
    fmt.Println("\nTransforming team data...")
    transformed := []TransformedTeam{}

    for _, team := range e.Teams {
        if team.TeamID == 0 || team.Name == "" {
            e.ValidationErrors = append(e.ValidationErrors,
                fmt.Sprintf("Team validation failed: missing required fields"))
            continue
        }

        region := mapCountryToRegion(team.Country)
        if region == "" {
            region = "Unknown"
        }

        transformed = append(transformed, TransformedTeam{
            TeamID:          team.TeamID,
            Name:            team.Name,
            Tag:             team.Tag,
            Region:          region,
            EstablishedDate: "2000-01-01", // default for now
        })
    }

    fmt.Printf("  Success:  Transformed %d teams\n", len(transformed))
    e.TransformationLog = append(e.TransformationLog,
        fmt.Sprintf("Teams: %d records transformed", len(transformed)))
    return transformed
}

func (e *ETLPipeline) TransformMatches(teams []TransformedTeam) []TransformedMatch {
    fmt.Println("\nTransforming match data...")
    transformed := []TransformedMatch{}

    teamMap := make(map[int]bool)
    for _, t := range teams {
        teamMap[t.TeamID] = true
    }

    for _, match := range e.Matches {
        errors := []string{}

        if match.MatchID == 0 {
            errors = append(errors, "missing match_id")
        }

        if match.RadiantTeamID == 0 || match.DireTeamID == 0 {
            errors = append(errors, "missing team IDs")
        } else {
            if !teamMap[match.RadiantTeamID] || !teamMap[match.DireTeamID] {
                errors = append(errors, "team not found in teams table")
            }
        }

        if match.RadiantTeamID == match.DireTeamID {
            errors = append(errors, "team cannot play against itself")
        }

        if match.StartTime == 0 {
            errors = append(errors, "invalid match start time")
        }

        if len(errors) > 0 {
            e.ValidationErrors = append(e.ValidationErrors,
                fmt.Sprintf("Match %d: %s", match.MatchID, strings.Join(errors, ", ")))
            continue
        }

        var winnerID int
        if match.RadiantWin {
            winnerID = match.RadiantTeamID
        } else {
            winnerID = match.DireTeamID
        }

        matchDate := time.Unix(match.StartTime, 0).Format("2006-01-02")
        transformed = append(transformed, TransformedMatch{
            MatchID:      match.MatchID,
            LeagueID:     match.LeagueID,
            Team1ID:      match.RadiantTeamID,
            Team2ID:      match.DireTeamID,
            WinnerTeamID: winnerID,
            MatchDate:    matchDate,
            Duration:     match.Duration,
            MapOrGame:    "Dota 2",
        })
    }

    fmt.Printf("  Success:  Transformed %d matches\n", len(transformed))
    e.TransformationLog = append(e.TransformationLog,
        fmt.Sprintf("Matches: %d records transformed", len(transformed)))
    return transformed
}

func (e *ETLPipeline) TransformLeagues() []TransformedGame {
    fmt.Println("\nTransforming league data...")
    transformed := []TransformedGame{}

    transformed = append(transformed, TransformedGame{
        GameID:      1,
        Name:        "Dota 2",
        Genre:       "MOBA",
        MaxTeamSize: 5,
    })

    fmt.Printf("  Success:  Derived game definitions\n")
    e.TransformationLog = append(e.TransformationLog,
        fmt.Sprintf("Games: %d record(s) derived", len(transformed)))
    return transformed
}

// ============== loader ==============

func SaveTeamsToCSV(filename string, teams []TransformedTeam) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    writer := csv.NewWriter(file)
    defer writer.Flush()

    if err := writer.Write([]string{"TeamId", "Name", "Tag", "Region", "EstablishedDate"}); err != nil {
        return err
    }

    for _, team := range teams {
        if err := writer.Write([]string{
            strconv.Itoa(team.TeamID),
            team.Name,
            team.Tag,
            team.Region,
            team.EstablishedDate,
        }); err != nil {
            return err
        }
    }

    return nil
}

func SaveMatchesToCSV(filename string, matches []TransformedMatch) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    writer := csv.NewWriter(file)
    defer writer.Flush()

    if err := writer.Write([]string{"MatchId", "LeagueId", "Team1Id", "Team2Id", "WinnerTeamId", "MatchDate", "Duration", "MapOrGame"}); err != nil {
        return err
    }

    for _, match := range matches {
        if err := writer.Write([]string{
            strconv.FormatInt(match.MatchID, 10),
            strconv.Itoa(match.LeagueID),
            strconv.Itoa(match.Team1ID),
            strconv.Itoa(match.Team2ID),
            strconv.Itoa(match.WinnerTeamID),
            match.MatchDate,
            strconv.Itoa(match.Duration),
            match.MapOrGame,
        }); err != nil {
            return err
        }
    }

    return nil
}

func SaveTeamsToJSON(filename string, teams []TransformedTeam) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    return json.NewEncoder(file).Encode(teams)
}

func SaveMatchesToJSON(filename string, matches []TransformedMatch) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    return json.NewEncoder(file).Encode(matches)
}

// ============== helpers ==============

func mapCountryToRegion(countryCode string) string {
    regions := map[string]string{
        "CN": "CN",
        "KR": "KR",
        "JP": "KR",
        "TW": "CN",
        "US": "NA",
        "CA": "NA",
        "MX": "NA",
        "BR": "SA",
        "AR": "SA",
        "SE": "EU",
        "NO": "EU",
        "DE": "EU",
        "FR": "EU",
        "GB": "EU",
        "RU": "CIS",
        "UA": "CIS",
        "TR": "EU",
        "AU": "AU",
        "NZ": "AU",
        "SG": "SEA",
        "TH": "SEA",
        "PH": "SEA",
        "ID": "SEA",
        "MY": "SEA",
    }

    if region, ok := regions[strings.ToUpper(countryCode)]; ok {
        return region
    }
    return ""
}

// ============== orchestration ==============

func (e *ETLPipeline) Run() error {
    fmt.Println("=" + strings.Repeat("=", 68) + "=")
    fmt.Println("TOURNAMENT SYSTEM ETL PIPELINE - PROOF OF CONCEPT (Go)")
    fmt.Println("Data Source: OpenDota API + Realistic Match Data Generation")
    fmt.Println("=" + strings.Repeat("=", 68) + "=")

    fmt.Println("\n[1/3] EXTRACT PHASE")
    fmt.Println("-" + strings.Repeat("-", 68) + "-")

    if err := e.ExtractTeams(); err != nil {
        return fmt.Errorf("extraction failed: %w", err)
    }

    if err := e.ExtractLeagues(); err != nil {
        return fmt.Errorf("extraction failed: %w", err)
    }

    if err := e.ExtractMatches(50); err != nil {
        return fmt.Errorf("extraction failed: %w", err)
    }

    fmt.Println("\n[2/3] TRANSFORM PHASE")
    fmt.Println("-" + strings.Repeat("-", 68) + "-")

    teams := e.TransformTeams()
    games := e.TransformLeagues()
    matches := e.TransformMatches(teams)

    fmt.Println("\n[3/3] LOAD PHASE (CSV + JSON Export)")
    fmt.Println("-" + strings.Repeat("-", 68) + "-")

    os.MkdirAll("transformed_data", 0755)

    fmt.Println("Exporting teams to CSV...")
    if err := SaveTeamsToCSV("transformed_data/teams.csv", teams); err != nil {
        return err
    }
    fmt.Println("  Success:  Saved to transformed_data/teams.csv")

    fmt.Println("Exporting matches to CSV...")
    if err := SaveMatchesToCSV("transformed_data/matches.csv", matches); err != nil {
        return err
    }
    fmt.Println("  Success:  Saved to transformed_data/matches.csv")

    fmt.Println("Exporting teams to JSON...")
    if err := SaveTeamsToJSON("transformed_data/teams.json", teams); err != nil {
        return err
    }
    fmt.Println("  Success:  Saved to transformed_data/teams.json")

    fmt.Println("Exporting matches to JSON...")
    if err := SaveMatchesToJSON("transformed_data/matches.json", matches); err != nil {
        return err
    }
    fmt.Println("  Success:  Saved to transformed_data/matches.json")

    // summary
    fmt.Println("\n" + "=" + strings.Repeat("=", 68) + "=")
    fmt.Println("PIPELINE EXECUTION SUMMARY")
    fmt.Println("=" + strings.Repeat("=", 68) + "=")

    fmt.Println("\nTransformation Log:")
    for _, log := range e.TransformationLog {
        fmt.Printf("  • %s\n", log)
    }

    fmt.Printf("\nValidation Errors: %d\n", len(e.ValidationErrors))
    if len(e.ValidationErrors) > 0 {
        for i, err := range e.ValidationErrors {
            if i >= 5 {
                fmt.Printf("  ... and %d more\n", len(e.ValidationErrors)-5)
                break
            }
            fmt.Printf("  • %s\n", err)
        }
    } else {
        fmt.Println("  Success:  No validation errors")
    }

    fmt.Println("\nData Summary:")
    fmt.Printf("  • Total Teams Extracted: %d\n", len(e.Teams))
    fmt.Printf("  • Teams Transformed: %d\n", len(teams))
    fmt.Printf("  • Total Matches Extracted: %d\n", len(e.Matches))
    fmt.Printf("  • Matches Transformed: %d\n", len(matches))
    fmt.Printf("  • Games Derived: %d\n", len(games))

    fmt.Println("\nSample Data:")
    if len(teams) > 0 {
        fmt.Printf("  Sample Team: %s (%s) - Region: %s\n",
            teams[0].Name, teams[0].Tag, teams[0].Region)
    }
    if len(matches) > 0 {
        fmt.Printf("  Sample Match: Team %d vs Team %d → Winner: %d (%s)\n",
            matches[0].Team1ID, matches[0].Team2ID, matches[0].WinnerTeamID, matches[0].MatchDate)
    }

    fmt.Println("\n" + "=" + strings.Repeat("=", 68) + "=")
    fmt.Println("Success:  PIPELINE COMPLETE - Data exported to transformed_data/")
    fmt.Println("=" + strings.Repeat("=", 68) + "=")

    return nil
}

func main() {
    pipeline := NewETLPipeline()
    if err := pipeline.Run(); err != nil {
        fmt.Fprintf(os.Stderr, "Pipeline error: %v\n", err)
        os.Exit(1)
    }
}
