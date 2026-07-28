// net_checkins.go — Net check-in tracking, streaks, and stats.
//
// Detects check-ins by watching outgoing APRS messages addressed to any of
// the known net destinations (ANSRVR, APRSPH, 9M4GKS — same list the web
// UI's Quick Net Check-in menu uses, see KNOWN_NETS in index.html) and
// records them per-callsign, per-net. Persisted to net_checkins.json so
// streaks and history survive a restart.
package main

import (
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Mirrors index.html's KNOWN_NETS — kept in sync manually since one lives
// in Go and the other in the embedded frontend. Destination is the
// callsign a check-in message is addressed TO; Name is the display name
// used in stats/leaderboards.
type netDefinition struct {
	Name        string
	Destination string
}

var knownNets = []netDefinition{
	{Name: "APRS Thursday (HOTG)", Destination: "ANSRVR"},
	{Name: "APRSPH Thursday", Destination: "APRSPH"},
	{Name: "APRSMY", Destination: "APRSMY"},
	{Name: "Hamfinity Sunday", Destination: "9M4GKS"},
}

// One recorded check-in.
type NetCheckin struct {
	Timestamp int64  `json:"ts"`
	Callsign  string `json:"callsign"` // full callsign incl. SSID, as heard
	Net       string `json:"net"`      // matches netDefinition.Name
	Software  string `json:"software,omitempty"`
	Text      string `json:"text"`
}

var (
	netCheckinsMu sync.RWMutex
	netCheckins   []NetCheckin
)

const netCheckinsFile = "net_checkins.json"

func loadNetCheckins() {
	data, err := os.ReadFile(netCheckinsFile)
	if err != nil {
		return // fine — starts empty, matches loadSavedConfig's pattern
	}
	var loaded []NetCheckin
	if err := json.Unmarshal(data, &loaded); err != nil {
		return
	}
	netCheckinsMu.Lock()
	netCheckins = loaded
	netCheckinsMu.Unlock()
}

func saveNetCheckins() {
	netCheckinsMu.RLock()
	data, err := json.MarshalIndent(netCheckins, "", "  ")
	netCheckinsMu.RUnlock()
	if err != nil {
		return
	}
	os.WriteFile(netCheckinsFile, data, 0644)
}

// netForDestination returns the matching net definition for a message's
// "to" callsign, or nil if it doesn't match any known net.
func netForDestination(to string) *netDefinition {
	to = strings.ToUpper(strings.TrimSpace(to))
	for i := range knownNets {
		if knownNets[i].Destination == to {
			return &knownNets[i]
		}
	}
	return nil
}

// recordNetCheckinIfMatch checks a parsed message against the known nets
// and, if it matches, records a check-in. Software is identified from the
// message packet's own TOCALL (the destination field in FROM>TOCALL,PATH:,
// e.g. APDR16 = APRSdroid) via the embedded aprs-deviceid database —
// every APRS packet self-identifies its originating software this way,
// so this covers check-ins from anywhere on the network, not just clients
// directly connected to this server.
func recordNetCheckinIfMatch(from, to, text, tocall string) {
	net := netForDestination(to)
	if net == nil {
		return
	}
	entry := NetCheckin{
		Timestamp: time.Now().Unix(),
		Callsign:  strings.ToUpper(strings.TrimSpace(from)),
		Net:       net.Name,
		Software:  lookupDeviceByTocall(tocall),
		Text:      text,
	}
	netCheckinsMu.Lock()
	netCheckins = append(netCheckins, entry)
	// Cap history to the most recent 20,000 entries so the file/memory
	// doesn't grow unbounded forever — generous enough for years of a
	// weekly net at normal participation levels.
	if len(netCheckins) > 20000 {
		netCheckins = netCheckins[len(netCheckins)-20000:]
	}
	netCheckinsMu.Unlock()
	go saveNetCheckins()
}

// ─── Stats computation ──────────────────────────────────────────────────

type OperatorStat struct {
	Callsign  string `json:"callsign"`
	Checkins  int    `json:"checkins"`
	LastNet   string `json:"last_net"`
	LastTs    int64  `json:"last_ts"`
	Streak    int    `json:"streak_weeks"`
	FavHour   int    `json:"fav_hour"` // 0-23, UTC
	Device    string `json:"primary_device"`
}

// weeksSinceEpoch buckets a unix timestamp into an ISO-week-like bucket
// (7-day blocks from the Unix epoch) for streak computation. Not
// calendar-week-aligned, but consistent and simple — a streak means
// "checked in at least once in each of N consecutive 7-day windows".
func weekBucket(ts int64) int64 {
	return ts / (7 * 24 * 3600)
}

// computeOperatorStats builds one OperatorStat per callsign that has ever
// checked in, from the full history. Called on-demand by the stats API,
// not cached — the history is bounded (see the 20k cap above) so this is
// cheap enough to run per-request.
func computeOperatorStats() []OperatorStat {
	netCheckinsMu.RLock()
	entries := make([]NetCheckin, len(netCheckins))
	copy(entries, netCheckins)
	netCheckinsMu.RUnlock()

	type agg struct {
		count      int
		lastNet    string
		lastTs     int64
		weeks      map[int64]bool
		hourCounts [24]int
		deviceCnt  map[string]int
	}
	byCall := map[string]*agg{}
	for _, e := range entries {
		a, ok := byCall[e.Callsign]
		if !ok {
			a = &agg{weeks: map[int64]bool{}, deviceCnt: map[string]int{}}
			byCall[e.Callsign] = a
		}
		a.count++
		if e.Timestamp > a.lastTs {
			a.lastTs = e.Timestamp
			a.lastNet = e.Net
		}
		a.weeks[weekBucket(e.Timestamp)] = true
		hr := time.Unix(e.Timestamp, 0).UTC().Hour()
		a.hourCounts[hr]++
		if e.Software != "" {
			a.deviceCnt[e.Software]++
		}
	}

	out := make([]OperatorStat, 0, len(byCall))
	for call, a := range byCall {
		// Streak: count back from the most recent week bucket the
		// operator has, as long as consecutive buckets are all present.
		streak := 0
		cur := weekBucket(a.lastTs)
		for a.weeks[cur] {
			streak++
			cur--
		}
		favHour, favCount := 0, -1
		for h := 0; h < 24; h++ {
			if a.hourCounts[h] > favCount {
				favCount = a.hourCounts[h]
				favHour = h
			}
		}
		device, deviceCount := "", -1
		for d, c := range a.deviceCnt {
			if c > deviceCount {
				deviceCount = c
				device = d
			}
		}
		out = append(out, OperatorStat{
			Callsign: call,
			Checkins: a.count,
			LastNet:  a.lastNet,
			LastTs:   a.lastTs,
			Streak:   streak,
			FavHour:  favHour,
			Device:   device,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Checkins > out[j].Checkins })
	return out
}

// deviceUsageBreakdown returns a callsign-agnostic count of check-ins per
// software/device string, for the "Device Usage" chart.
func deviceUsageBreakdown() map[string]int {
	netCheckinsMu.RLock()
	defer netCheckinsMu.RUnlock()
	out := map[string]int{}
	for _, e := range netCheckins {
		if e.Software == "" {
			continue
		}
		out[e.Software]++
	}
	return out
}

// weeklyTrend returns check-in counts for each of the last N weeks
// (oldest first), for the "Weekly Trend" chart.
func weeklyTrend(weeks int) []int {
	netCheckinsMu.RLock()
	defer netCheckinsMu.RUnlock()
	nowWeek := weekBucket(time.Now().Unix())
	counts := make([]int, weeks)
	for _, e := range netCheckins {
		w := weekBucket(e.Timestamp)
		idx := int(nowWeek - w)
		if idx >= 0 && idx < weeks {
			counts[weeks-1-idx]++
		}
	}
	return counts
}

// hourlyDistribution returns check-in counts per UTC hour (0-23) across
// the last N days, for the "Hourly Distribution" chart.
func hourlyDistribution(days int) [24]int {
	netCheckinsMu.RLock()
	defer netCheckinsMu.RUnlock()
	cutoff := time.Now().Unix() - int64(days)*24*3600
	var counts [24]int
	for _, e := range netCheckins {
		if e.Timestamp < cutoff {
			continue
		}
		counts[time.Unix(e.Timestamp, 0).UTC().Hour()]++
	}
	return counts
}

// ─── TOCALL -> software lookup (reuses the embedded aprs-deviceid table
// that /api/tocalls already serves to the frontend) ─────────────────────

type tocallEntry struct {
	Tocall string `json:"tocall"`
	Vendor string `json:"vendor"`
	Model  string `json:"model"`
}

var (
	tocallTableOnce sync.Once
	tocallTable     []tocallEntry
)

func loadTocallTable() {
	var doc struct {
		Tocalls []tocallEntry `json:"tocalls"`
	}
	if err := json.Unmarshal(embeddedTocallsJSON, &doc); err == nil {
		tocallTable = doc.Tocalls
	}
}

// tocallMatches checks a real TOCALL (e.g. "APDR16") against a pattern
// that may contain '?' wildcards (e.g. "APDR??"), same semantics as the
// aprs-deviceid database uses.
func tocallMatches(pattern, real string) bool {
	if len(pattern) != len(real) {
		return false
	}
	for i := 0; i < len(pattern); i++ {
		if pattern[i] != '?' && pattern[i] != real[i] {
			return false
		}
	}
	return true
}

// lookupDeviceByTocall returns "Vendor Model" for a TOCALL, or "" if it
// isn't in the database or the tocall is empty.
func lookupDeviceByTocall(tocall string) string {
	if tocall == "" {
		return ""
	}
	tocallTableOnce.Do(loadTocallTable)
	tocall = strings.ToUpper(tocall)
	for _, e := range tocallTable {
		if tocallMatches(strings.ToUpper(e.Tocall), tocall) {
			if e.Vendor != "" && e.Model != "" {
				return e.Vendor + " " + e.Model
			}
			if e.Model != "" {
				return e.Model
			}
			return e.Vendor
		}
	}
	return ""
}

// ─── HTTP handlers ───────────────────────────────────────────────────────

// GET /api/netcheckins/stats — dashboard summary: totals, weekly trend,
// hourly distribution, device usage. No auth (matches /api/status).
func handleNetCheckinStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	netCheckinsMu.RLock()
	total := len(netCheckins)
	uniqueOps := map[string]bool{}
	var latestTs int64
	var latestCall string
	for _, e := range netCheckins {
		uniqueOps[e.Callsign] = true
		if e.Timestamp > latestTs {
			latestTs = e.Timestamp
			latestCall = e.Callsign
		}
	}
	netCheckinsMu.RUnlock()

	weekStart := time.Now().Unix() - int64(time.Now().UTC().Weekday())*86400
	netCheckinsMu.RLock()
	thisWeek := 0
	for _, e := range netCheckins {
		if e.Timestamp >= weekStart {
			thisWeek++
		}
	}
	netCheckinsMu.RUnlock()

	resp := map[string]interface{}{
		"total_checkins": total,
		"unique_ops":     len(uniqueOps),
		"this_week":      thisWeek,
		"latest_call":    latestCall,
		"latest_ts":      latestTs,
		"weekly_trend":   weeklyTrend(12),
		"hourly_dist":    hourlyDistribution(7),
		"device_usage":   deviceUsageBreakdown(),
	}
	json.NewEncoder(w).Encode(resp)
}

// GET /api/netcheckins/leaderboard?limit=20 — top operators by check-in
// count, with streak/fav-hour/device.
func handleNetCheckinLeaderboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	limit := 20
	if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 && l <= 200 {
		limit = l
	}
	stats := computeOperatorStats()
	if len(stats) > limit {
		stats = stats[:limit]
	}
	json.NewEncoder(w).Encode(stats)
}

// GET /api/netcheckins/me?callsign=M0ABC — personal stats for one
// operator (streak, favourite hour, primary device, total check-ins).
func handleNetCheckinMe(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	call := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("callsign")))
	if call == "" {
		http.Error(w, `{"error":"callsign required"}`, http.StatusBadRequest)
		return
	}
	for _, s := range computeOperatorStats() {
		if s.Callsign == call {
			json.NewEncoder(w).Encode(s)
			return
		}
	}
	json.NewEncoder(w).Encode(OperatorStat{Callsign: call})
}

// GET /api/netcheckins/archive?callsign=&year=&month=&limit=100 — searchable
// check-in history, newest first.
func handleNetCheckinArchive(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	q := r.URL.Query()
	callFilter := strings.ToUpper(strings.TrimSpace(q.Get("callsign")))
	year, _ := strconv.Atoi(q.Get("year"))
	month, _ := strconv.Atoi(q.Get("month"))
	limit := 100
	if l, err := strconv.Atoi(q.Get("limit")); err == nil && l > 0 && l <= 1000 {
		limit = l
	}

	netCheckinsMu.RLock()
	matches := make([]NetCheckin, 0, limit)
	for i := len(netCheckins) - 1; i >= 0 && len(matches) < limit; i-- {
		e := netCheckins[i]
		if callFilter != "" && !strings.HasPrefix(e.Callsign, callFilter) {
			continue
		}
		if year > 0 || month > 0 {
			t := time.Unix(e.Timestamp, 0).UTC()
			if year > 0 && t.Year() != year {
				continue
			}
			if month > 0 && int(t.Month()) != month {
				continue
			}
		}
		matches = append(matches, e)
	}
	netCheckinsMu.RUnlock()

	sort.Slice(matches, func(i, j int) bool { return matches[i].Timestamp > matches[j].Timestamp })
	json.NewEncoder(w).Encode(matches)
}
