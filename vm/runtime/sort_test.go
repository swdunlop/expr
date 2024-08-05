package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func BenchmarkSort(b *testing.B) {
	loadMeteorites()
	testSort(b, `Masses`, meteorites.Masses)
	testSort(b, `Times`, meteorites.Times)
	testSort(b, `Names`, meteorites.Names)
	testSort(b, `SIDs`, meteorites.SIDs)
	testSortBy(b, `ByMasses`, meteorites.Masses)
	testSortBy(b, `ByTimes`, meteorites.Times)
	testSortBy(b, `ByNames`, meteorites.Names)
	testSortBy(b, `BySIDs`, meteorites.SIDs)
}

func testSort(b *testing.B, name string, src []any) {
	b.Helper()
	b.Run(name, func(b *testing.B) {
		tmp := make([]any, len(src))
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			copy(tmp, src)
			set := Sort{Array: tmp}
			b.StartTimer()
			sort.Sort(&set)
		}
	})
}

func testSortBy(b *testing.B, name string, src []any) {
	b.Helper()
	b.Run(name, func(b *testing.B) {
		tmp := make([]any, len(src))
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			copy(tmp, src)
			set := SortBy{Array: meteorites.Records, Values: tmp}
			b.StartTimer()
			sort.Sort(&set)
		}
	})
}

func copyArray(src []any) []any {
	return src
}

func loadMeteorites() {
	js, err := os.ReadFile(filepath.Join(`data`, `rows.json`))
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(js, &meteorites)
	if err != nil {
		panic(err)
	}

	type record struct {
		SID  string
		Name string
		Mass float64
		Time time.Time
	}

	meteorites.Records = make([]any, 0, len(meteorites.Rows))
	meteorites.Masses = make([]any, 0, len(meteorites.Rows))
	meteorites.Times = make([]any, 0, len(meteorites.Rows))
	meteorites.Names = make([]any, 0, len(meteorites.Rows))
	meteorites.SIDs = make([]any, 0, len(meteorites.Rows))
	for _, row := range meteorites.Rows {
		var rec record
		if str, ok := row[0].(string); ok {
			rec.SID = str
		}
		if str, ok := row[8].(string); ok {
			rec.Name = str
		}
		if num, ok := row[12].(float64); ok {
			rec.Mass = num
		}
		if str, ok := row[14].(string); ok {
			ts, err := time.Parse(time.RFC3339, str)
			if err == nil {
				rec.Time = ts
			}
		}
		meteorites.SIDs = append(meteorites.SIDs, rec.SID)
		meteorites.Names = append(meteorites.Names, rec.Name)
		meteorites.Masses = append(meteorites.Masses, rec.Mass)
		meteorites.Times = append(meteorites.Times, rec.Time)
		meteorites.Records = append(meteorites.Records, rec)
	}
}

// loadMeteoritesOnce guards against repeatedly calling loadMeteorites.
var loadMeteoritesOnce sync.Once

// meteorites dataset from data/rows.json, downloaded from https://data.nasa.gov/api/views/gh4g-9sfh/rows.json
var meteorites struct {
	Rows [][]any `json:"rows"`

	// Records is synthesized from Mass, Time, Name, SID.
	Records []any `json:"records"`

	// SIDs is extracted by loadMeteorites from Rows[..][0].
	// This is a unique per-row identifier and is random in distribution with a common prefix.
	SIDs []any `json:"sids"`

	// Names is extracted by loadMeteorites from Rows[..][8].
	// This is the list of names and is mostly sorted, differing on UTF-8 anomalies.
	Names []any `json:"names"`

	// Masses is extracted by loadMeteorites from Rows [..][12].
	// This is the mass of the meteorite, in grams and is fairly random in distribution.
	Masses []any `json:"masses"`

	// Times is extracted by loadMeteorites from Rows[..][14].
	// This is the time of the event, ISO-8601 and is somewhat sorted in distribution.
	Times []any `json:"times"`
}
