package main

// sota_pota.go — SOTA Spotter and POTA live activation integration.
//
// Fetches live activations every 5 minutes from:
//   SOTA: https://api2.sota.org.uk/api/spots/10/-1   (last 10 spots, all bands)
//   POTA: https://api.pota.app/spot/activator        (all current spots)
//
// Cross-references with APRS stations in the packet history and sends a
// push notification + WS message when a SOTA/POTA activator is heard.
//
// API: GET /api/sota-pota — returns current spot list for map overlay.

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ── Data types ─────────────────────────────────────────────────────────────

type SOTASpot struct {
	ID          int    `json:"id"`
	Callsign    string `json:"activatorCallsign"`
	SummitCode  string `json:"summitCode"`
	SummitName  string `json:"summitName"`
	Frequency   string `json:"frequency"`
	Mode        string `json:"mode"`
	TimeStamp   string `json:"timeStamp"`
}

type POTASpot struct {
	Callsign    string `json:"activator"`
	ParkCode    string `json:"reference"`
	ParkName    string `json:"name"`
	Frequency   string `json:"frequency"`
	Mode        string `json:"mode"`
	SpotTime    string `json:"spotTime"`
	Comments    string `json:"comments"`
}

// SotaPotaEntry is the unified format served to the web map.
type SotaPotaEntry struct {
	Callsign    string  `json:"callsign"`
	Type        string  `json:"type"`       // "sota" | "pota"
	Reference   string  `json:"reference"`  // summit code or park code
	Name        string  `json:"name"`
	Frequency   string  `json:"frequency"`
	Mode        string  `json:"mode"`
	SpotTime    string  `json:"spot_time"`
	OnAPRS      bool    `json:"on_aprs"`    // true if heard on APRS
	APRSLat     float64 `json:"aprs_lat,omitempty"`
	APRSLon     float64 `json:"aprs_lon,omitempty"`
}

var (
	sotaPotaMu      sync.RWMutex
	sotaPotaSpots   []SotaPotaEntry
	sotaPotaAlerted = map[string]int64{} // callsign → last alert unix time
)

// ── Fetch loop ─────────────────────────────────────────────────────────────

func runSotaPotaFetcher() {
	tick := time.NewTicker(5 * time.Minute)
	defer tick.Stop()
	fetchSotaPota() // immediate first fetch
	for range tick.C {
		fetchSotaPota()
	}
}

func fetchSotaPota() {
	var spots []SotaPotaEntry

	// SOTA
	if sota, err := fetchSOTA(); err == nil {
		spots = append(spots, sota...)
	} else {
		log.Printf("SOTA fetch: %v", err)
	}

	// POTA
	if pota, err := fetchPOTA(); err == nil {
		spots = append(spots, pota...)
	} else {
		log.Printf("POTA fetch: %v", err)
	}

	// Cross-reference with APRS stations
	for i := range spots {
		call := strings.ToUpper(spots[i].Callsign)
		if lat, lon, found := latestStationPosition(call); found {
			spots[i].OnAPRS = true
			spots[i].APRSLat = lat
			spots[i].APRSLon = lon
		}
	}

	sotaPotaMu.Lock()
	sotaPotaSpots = spots
	sotaPotaMu.Unlock()

	// Notify members who watch any of these callsigns
	notifySotaPota(spots)
}

func fetchSOTA() ([]SotaPotaEntry, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("https://api2.sota.org.uk/api/spots/10/-1")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var raw []SOTASpot
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	var out []SotaPotaEntry
	for _, s := range raw {
		out = append(out, SotaPotaEntry{
			Callsign:  strings.ToUpper(strings.TrimSpace(s.Callsign)),
			Type:      "sota",
			Reference: s.SummitCode,
			Name:      s.SummitName,
			Frequency: s.Frequency,
			Mode:      s.Mode,
			SpotTime:  s.TimeStamp,
		})
	}
	return out, nil
}

func fetchPOTA() ([]SotaPotaEntry, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("https://api.pota.app/spot/activator")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var raw []POTASpot
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	var out []SotaPotaEntry
	for _, s := range raw {
		out = append(out, SotaPotaEntry{
			Callsign:  strings.ToUpper(strings.TrimSpace(s.Callsign)),
			Type:      "pota",
			Reference: s.ParkCode,
			Name:      s.ParkName,
			Frequency: s.Frequency,
			Mode:      s.Mode,
			SpotTime:  s.SpotTime,
		})
	}
	return out, nil
}

// notifySotaPota alerts members who are watching any activating callsign.
func notifySotaPota(spots []SotaPotaEntry) {
	now := time.Now().Unix()
	memberStoreMu.RLock()
	members := memberStore.Members
	memberStoreMu.RUnlock()

	for _, spot := range spots {
		call := spot.Callsign
		// Rate-limit: one notification per callsign per 30 minutes
		if last, ok := sotaPotaAlerted[call]; ok && now-last < 1800 {
			continue
		}

		label := "SOTA"
		if spot.Type == "pota" {
			label = "POTA"
		}

		for _, m := range members {
			for _, watched := range m.Watchlist {
				if strings.EqualFold(watched, call) {
					go pushToMember(m.ID, PushPayload{
						Type:  "sota_pota",
						Title: label + " Alert: " + call,
						Body:  call + " active at " + spot.Reference + " (" + spot.Name + ") on " + spot.Frequency + " " + spot.Mode,
						Tag:   "sota_pota_" + call,
					})
					go pushAlertToMember(m.Callsign, "sota_pota", call,
						label+": "+call+" active at "+spot.Reference+" on "+spot.Frequency+" "+spot.Mode)
					break
				}
			}
		}
		sotaPotaAlerted[call] = now
	}
}

// ── HTTP: GET /api/sota-pota ───────────────────────────────────────────────

func handleSotaPota(w http.ResponseWriter, r *http.Request) {
	sotaPotaMu.RLock()
	spots := append([]SotaPotaEntry(nil), sotaPotaSpots...)
	sotaPotaMu.RUnlock()
	if spots == nil {
		spots = []SotaPotaEntry{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(spots)
}
