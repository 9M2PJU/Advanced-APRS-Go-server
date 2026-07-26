package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// AppVersion is the running server version, compared against GitHub releases.
const AppVersion = "2.3.1"

type AppConfig struct {
	ServerName     string  `json:"server_name"`
	SoftwareVers   string  `json:"software_vers"`
	Callsign       string  `json:"callsign"`
	Passcode       string  `json:"passcode"`
	UpstreamAddr   string  `json:"upstream_addr"`
	ServerFilter   string  `json:"server_filter"`
	DropPiStar     bool    `json:"drop_pistar"`
	DropDStar      bool    `json:"drop_dstar"`
	DropAPDesk     bool    `json:"drop_apdesk"`
	EnableGeofence bool    `json:"enable_geofence"`
	CenterLat      float64 `json:"center_lat"`
	CenterLon      float64 `json:"center_lon"`
	RadiusKm       float64 `json:"radius_km"`
	AISStreamKey   string  `json:"ais_stream_key"`
	QRZUsername    string  `json:"qrz_username"`
	QRZPassword    string  `json:"qrz_password"`
	MetOfficeKey   string  `json:"met_office_key"`
	sync.RWMutex   `json:"-"`
}

// adminCreds holds the HTTP Basic Auth credentials for the admin panel.
// These are runtime-changeable without restarting.
var adminCreds struct {
	sync.RWMutex
	Username string
	Password string
}

var (
	config = AppConfig{
		ServerName:     "T2CUSTOM",
		SoftwareVers:   "AdvancedGoAPRS 12.0",
		Callsign:       "NOCALL",
		Passcode:       "-1",
		UpstreamAddr:   "rotate.aprs2.net:14580",
		ServerFilter:   "auto",
		DropPiStar:     false,
		DropDStar:      true,
		DropAPDesk:     true,
		EnableGeofence: false,
		CenterLat:      51.5,
		CenterLon:      -0.1,
		RadiusKm:       100.0,
		AISStreamKey:   "",
		QRZUsername:    "",
		QRZPassword:    "",
		MetOfficeKey:   "",
	}

	upstreamConnected int32
	upstreamTx        = make(chan string, 5000)

	metrics = struct {
		StartTime   time.Time
		PktsRx      uint64
		PktsTx      uint64
		PktsDropped uint64
		BytesRx     uint64
		BytesTx     uint64
		sync.RWMutex
	}{StartTime: time.Now()}

	clients       = make(map[*wsClient]bool)
	clientsMu     sync.RWMutex
	broadcast     = make(chan string, 5000)
	upstreamOut   = make(chan string, 5000)
	reconnectChan = make(chan struct{}, 1)

	upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	dupes   = make(map[string]time.Time)
	dupesMu sync.Mutex

	history    []HistoryPacket
	historyMu  sync.RWMutex
	maxHistory = 10000

	// rawRing stores the last hour of all raw packets for replay to new clients
	rawRing   []RawEntry
	rawRingMu sync.RWMutex

	// msgStore stores the last hour of APRS messages for replay
	msgStore   []MsgEntry
	msgStoreMu sync.RWMutex

	// objectStore holds active APRS objects and items
	objectStore   map[string]*ObjectEntry
	objectStoreMu sync.RWMutex

	// Webhooks and API keys
	webhooks   []WebhookConfig
	webhooksMu sync.RWMutex
	apiKeys    map[string]APIKey
	apiKeysMu  sync.RWMutex

	// Metrics counters
	totalPackets   int64
	dupePackets    int64
	upstreamBytes  int64
	packetsLastMin int64
	metricsStart   = time.Now()
)

var (
	// Optional 7-char timestamp (DDHHMMz / DDHHMM/ / HHMMSSh) follows the DTI
	// for @ and / position reports (used by WX beacons and most trackers).
	posRegex = regexp.MustCompile(`[!\/=@\*](?:\d{6}[zh\/])?(\d{2})(\d{2}\.\d{2})([NS])(.)(\d{3})(\d{2}\.\d{2})([EW])(.)`)
	// Compressed position: DTI + sym_table(1) + lat_b91(4) + lon_b91(4) + sym(1) + cs(2) + T(1)
	// Used by all OE5BPA / LoRa_APRS_Tracker / ESP32 LoRa nodes.
	// Same optional timestamp as posRegex — @/ DTI compressed reports also
	// carry a DDHHMMz/DDHHMM//HHMMSSh timestamp before the compressed block.
	compPosRegex = regexp.MustCompile(`[!\/=@\*](?:\d{6}[zh\/])?([\/\\0-9A-Z])([\x21-\x7b]{4})([\x21-\x7b]{4})([\x21-\x7b])([\x20-\x7b]{2})([\x21-\x7b])`)
	// Object format: ;NAME_____*DDHHMMzDDMM.MMN/DDDMM.MMW symbol comment
	objRegex = regexp.MustCompile(`;([^\*]{9})[\*_](\d{6}[z\/])(\d{2})(\d{2}\.\d{2})([NS])(.)(\d{3})(\d{2}\.\d{2})([EW])(.)`)
	// Item format: )NAME!DDMM.MMN/DDDMM.MMW symbol comment
	itemRegex = regexp.MustCompile(`\)([^!]{3,9})[!_](\d{2})(\d{2}\.\d{2})([NS])(.)(\d{3})(\d{2}\.\d{2})([EW])(.)`)
	phgRegex  = regexp.MustCompile(`PHG(\d)(\d)(\d)(\d)`)
)

// --- per-IP connection limiting -----------------------------------------
// Max concurrent WS+TCP connections allowed from a single IP address.
// Prevents a single host from exhausting server resources.
const maxConnsPerIP = 10

var (
	ipConnsMu sync.Mutex
	ipConns   = make(map[string]int)
)

// getRealIP extracts the client's real IP from a proxied request.
// Caddy (and most reverse proxies) set X-Real-IP to the original client IP.
// Falls back to X-Forwarded-For first entry, then raw RemoteAddr.
// Used by the WS per-IP limiter so the limit is per-client, not per-proxy.
func getRealIP(r *http.Request) string {
	if ip := r.Header.Get("X-Real-IP"); ip != "" {
		return strings.TrimSpace(ip)
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// X-Forwarded-For may be a comma-separated list; take the first (client) entry
		if idx := strings.Index(xff, ","); idx != -1 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func ipConnAllow(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ipConnsMu.Lock()
	defer ipConnsMu.Unlock()
	if ipConns[host] >= maxConnsPerIP {
		return false
	}
	ipConns[host]++
	return true
}

func ipConnRelease(addr string) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ipConnsMu.Lock()
	if ipConns[host] > 0 {
		ipConns[host]--
	}
	if ipConns[host] == 0 {
		delete(ipConns, host)
	}
	ipConnsMu.Unlock()
}

// sanitisePacket strips ASCII control characters (except CR/LF/TAB) from
// a raw APRS line to prevent injection of null bytes, escape sequences, or
// other control characters that could corrupt log output or downstream parsing.
var ctrlRe = regexp.MustCompile(`[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]`)

func sanitisePacket(line string) string {
	return ctrlRe.ReplaceAllString(line, "")
}

// -------------------------------------------------------------------------

// decodeBase91Pos decodes a 4-char Base91 string to an integer value.
func decodeBase91Pos(s string) int {
	v := 0
	for _, c := range s {
		v = v*91 + int(c-33)
	}
	return v
}

type HistoryPacket struct {
	Timestamp int64   `json:"ts"`
	Callsign  string  `json:"call"`
	Path      string  `json:"path"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Symbol    string  `json:"sym"`
	PHG       string  `json:"phg,omitempty"`
	Raw       string  `json:"raw"`
}

type WebhookConfig struct {
	ID         string   `json:"id"`
	URL        string   `json:"url"`
	Callsign   string   `json:"callsign"`
	Events     []string `json:"events"`
	Secret     string   `json:"secret,omitempty"`
	Enabled    bool     `json:"enabled"`
	LastFired  int64    `json:"last_fired,omitempty"`
	LastStatus int      `json:"last_status,omitempty"`
}

type APIKey struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Key      string `json:"key"`
	Created  int64  `json:"created"`
	LastUsed int64  `json:"last_used,omitempty"`
	ReadOnly bool   `json:"read_only"`
}

type WebhookEvent struct {
	Event     string      `json:"event"`
	Timestamp int64       `json:"timestamp"`
	Callsign  string      `json:"callsign,omitempty"`
	Data      interface{} `json:"data"`
}

type ObjectEntry struct {
	Timestamp int64   `json:"ts"`
	Name      string  `json:"name"`
	Owner     string  `json:"owner"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Symbol    string  `json:"sym"`
	Comment   string  `json:"comment"`
	Killed    bool    `json:"killed"`
	Raw       string  `json:"raw"`
}

type RawEntry struct {
	Timestamp int64  `json:"ts"`
	Packet    string `json:"packet"`
}

type MsgEntry struct {
	Timestamp int64  `json:"ts"`
	From      string `json:"from"`
	To        string `json:"to"`
	Text      string `json:"text"`
	MsgID     string `json:"id,omitempty"`
	Packet    string `json:"packet"`
}

type wsClient struct {
	conn          *websocket.Conn
	send          chan []byte
	authenticated bool
	callsign      string
	software      string
	clientID      string
	remoteAddr    string
	connectedAt   int64
	lastTx        time.Time
}

type wsMessage struct {
	Type     string      `json:"type"`
	Callsign string      `json:"callsign,omitempty"`
	Passcode string      `json:"passcode,omitempty"`
	Software string      `json:"software,omitempty"`
	ClientID string      `json:"client_id,omitempty"`
	Status   string      `json:"status,omitempty"`
	Packet   string      `json:"packet,omitempty"`
	Data     interface{} `json:"data,omitempty"`
	Minutes  int         `json:"minutes,omitempty"`
}

// registerWSAuthentication records an authenticated WebSocket identity and
// returns older sockets from the same application installation. ClientID is a
// random, persistent per-install identifier supplied by newer clients. It lets
// us remove stale reconnects without treating separate phones behind the same
// NAT address as duplicates.
func registerWSAuthentication(current *wsClient, callsign, software, clientID string) []*wsClient {
	callsign = strings.ToUpper(strings.TrimSpace(callsign))
	software = strings.TrimSpace(software)
	clientID = strings.TrimSpace(clientID)
	if len(clientID) > 128 {
		clientID = clientID[:128]
	}

	var stale []*wsClient
	clientsMu.Lock()
	current.authenticated = true
	current.callsign = callsign
	current.software = software
	current.clientID = clientID
	if clientID != "" {
		for other := range clients {
			if other != current &&
				other.authenticated &&
				other.callsign == callsign &&
				other.software == software &&
				other.clientID == clientID {
				stale = append(stale, other)
			}
		}
	}
	clientsMu.Unlock()
	return stale
}

func closeStaleWSClients(stale []*wsClient) {
	for _, old := range stale {
		if old == nil || old.conn == nil {
			continue
		}
		_ = old.conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "replaced by newer connection"),
			time.Now().Add(time.Second),
		)
		_ = old.conn.Close()
	}
}

// handleTocalls serves the embedded APRS device identification database.
// Source: https://github.com/aprsorg/aprs-deviceid (CC BY-SA 2.0)
// Updated occasionally by re-embedding tocalls.json at build time.
func handleTocalls(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=86400") // 24h cache
	w.Write(embeddedTocallsJSON)
}

// ─── QRZ.com XML API proxy ────────────────────────────────────────────────────
// QRZ XML API requires login (username+password) to get a session key, then
// session key + callsign to look up. We do this server-side and:
//   1. Cache the session key (rotates when expires)
//   2. Cache lookup results (24h TTL)
//   3. Strip sensitive fields before returning to client
// Spec: https://www.qrz.com/docs/xml/current_spec.html

type qrzCache struct {
	sync.RWMutex
	SessionKey string
	SessionExp int64
	Lookups    map[string]qrzCachedLookup
}

type qrzCachedLookup struct {
	Data    map[string]string
	Fetched int64
}

var qrzC = &qrzCache{Lookups: make(map[string]qrzCachedLookup)}

// HTTP client for QRZ API - 10 second timeout
var qrzHTTP = &http.Client{Timeout: 10 * time.Second}

type qrzSession struct {
	XMLName xml.Name `xml:"QRZDatabase"`
	Session struct {
		Key     string `xml:"Key"`
		Error   string `xml:"Error"`
		Message string `xml:"Message"`
		SubExp  string `xml:"SubExp"`
		Remark  string `xml:"Remark"`
	} `xml:"Session"`
	Callsign struct {
		Call    string `xml:"call"`
		Fname   string `xml:"fname"`
		Name    string `xml:"name"`
		Addr1   string `xml:"addr1"`
		Addr2   string `xml:"addr2"`
		State   string `xml:"state"`
		Zip     string `xml:"zip"`
		Country string `xml:"country"`
		Grid    string `xml:"grid"`
		County  string `xml:"county"`
		Land    string `xml:"land"`
		Class   string `xml:"class"`
		Email   string `xml:"email"`
		URL     string `xml:"url"`
		Image   string `xml:"image"`
		Aliases string `xml:"aliases"`
		Lat     string `xml:"lat"`
		Lon     string `xml:"lon"`
		Bio     string `xml:"bio"`
		QSLMgr  string `xml:"qslmgr"`
		EQSL    string `xml:"eqsl"`
		MQSL    string `xml:"mqsl"`
		LOTW    string `xml:"lotw"`
		Born    string `xml:"born"`
		NameFmt string `xml:"name_fmt"`
		NickN   string `xml:"nickname"`
	} `xml:"Callsign"`
}

// qrzLogin obtains a new session key from QRZ. Holds qrzC write lock.
func qrzLogin() (string, error) {
	config.RLock()
	user, pass := config.QRZUsername, config.QRZPassword
	config.RUnlock()
	if user == "" || pass == "" {
		return "", fmt.Errorf("QRZ not configured")
	}
	url := fmt.Sprintf("https://xmldata.qrz.com/xml/current/?username=%s;password=%s;agent=AdvancedAPRS/1.4",
		url_encode(user), url_encode(pass))
	resp, err := qrzHTTP.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var s qrzSession
	if err := xml.Unmarshal(body, &s); err != nil {
		return "", err
	}
	if s.Session.Error != "" {
		return "", fmt.Errorf("QRZ login: %s", s.Session.Error)
	}
	if s.Session.Key == "" {
		return "", fmt.Errorf("QRZ login: empty key")
	}
	return s.Session.Key, nil
}

// url_encode minimally percent-encodes a string for QRZ URL parameter use
func url_encode(s string) string {
	r := strings.NewReplacer(
		" ", "%20", "&", "%26", "?", "%3F", "=", "%3D", "#", "%23",
		";", "%3B", "+", "%2B", "/", "%2F")
	return r.Replace(s)
}

// handleQRZLookup proxies a QRZ callsign lookup with caching.
// handleFirmwareProxy streams a GitHub release firmware asset through our
// own server so the browser-based Firmware Flasher can fetch it. Direct
// browser fetches to GitHub's release asset storage (Azure Blob, behind
// release-assets.githubusercontent.com) do NOT send CORS headers, so a
// cross-origin fetch() from aprsnet.uk always fails with "Failed to fetch"
// even though the file itself is public. Proxying server-side sidesteps
// that entirely - our server has no CORS restriction on its own outbound
// requests, and we control the CORS headers on the response we send back.
//
// Usage: /api/firmware-proxy?repo=OWNER/REPO&asset=FILENAME.bin
// Only allows the known firmware repos, to avoid becoming an open proxy.
var firmwareProxyHTTP = &http.Client{Timeout: 30 * time.Second}

var firmwareProxyAllowedRepos = map[string]bool{
	"2E0LXY/2E0LXY-LoRa-APRS-Tracker": true,
	"2E0LXY/2E0LXY-LoRa-APRS-iGate":   true,
}

func handleFirmwareProxy(w http.ResponseWriter, r *http.Request) {
	repo := r.URL.Query().Get("repo")
	asset := r.URL.Query().Get("asset")
	if repo == "" || asset == "" {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"repo and asset parameters required"}`))
		return
	}
	if !firmwareProxyAllowedRepos[repo] {
		w.WriteHeader(403)
		w.Write([]byte(`{"error":"repo not allowed"}`))
		return
	}
	// Basic filename sanity check - only expect our own .bin release assets
	if strings.ContainsAny(asset, "/\\") || !strings.HasSuffix(asset, ".bin") {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"invalid asset name"}`))
		return
	}

	// Resolve the latest release and find the matching asset (server-side
	// call to api.github.com - not subject to browser CORS at all).
	apiURL := "https://api.github.com/repos/" + repo + "/releases/latest"
	req, _ := http.NewRequest("GET", apiURL, nil)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "aprsnet.uk-firmware-proxy")
	resp, err := firmwareProxyHTTP.Do(req)
	if err != nil {
		w.WriteHeader(502)
		json.NewEncoder(w).Encode(map[string]string{"error": "release lookup failed: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	var rel struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name               string `json:"name"`
			BrowserDownloadURL string `json:"browser_download_url"`
			Size               int64  `json:"size"`
		} `json:"assets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		w.WriteHeader(502)
		json.NewEncoder(w).Encode(map[string]string{"error": "release parse failed"})
		return
	}

	var assetURL string
	var assetSize int64
	for _, a := range rel.Assets {
		if a.Name == asset {
			assetURL = a.BrowserDownloadURL
			assetSize = a.Size
			break
		}
	}
	if assetURL == "" {
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]string{"error": "asset not found in latest release " + rel.TagName})
		return
	}

	// Fetch the binary server-side (this request has no Origin header, so
	// GitHub's lack of CORS headers is irrelevant here) and stream it back.
	binReq, _ := http.NewRequest("GET", assetURL, nil)
	binReq.Header.Set("User-Agent", "aprsnet.uk-firmware-proxy")
	binResp, err := firmwareProxyHTTP.Do(binReq)
	if err != nil {
		w.WriteHeader(502)
		json.NewEncoder(w).Encode(map[string]string{"error": "asset download failed: " + err.Error()})
		return
	}
	defer binResp.Body.Close()
	if binResp.StatusCode != 200 {
		w.WriteHeader(502)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("asset fetch returned HTTP %d", binResp.StatusCode)})
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+asset+"\"")
	if assetSize > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", assetSize))
	}
	w.Header().Set("X-Firmware-Version", rel.TagName)
	io.Copy(w, binResp.Body)
}

func handleQRZLookup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	call := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("call")))
	if call == "" {
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"call parameter required"}`))
		return
	}
	// Strip SSID suffix - QRZ doesn't know about it
	if i := strings.Index(call, "-"); i > 0 {
		call = call[:i]
	}

	// Check cache (24h TTL)
	qrzC.RLock()
	if c, ok := qrzC.Lookups[call]; ok && time.Now().Unix()-c.Fetched < 86400 {
		qrzC.RUnlock()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"call": call, "cached": true, "data": c.Data,
		})
		return
	}
	qrzC.RUnlock()

	// Need to query QRZ - get or refresh session key
	qrzC.Lock()
	if qrzC.SessionKey == "" || time.Now().Unix() > qrzC.SessionExp {
		key, err := qrzLogin()
		if err != nil {
			qrzC.Unlock()
			w.WriteHeader(503)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		qrzC.SessionKey = key
		qrzC.SessionExp = time.Now().Unix() + 3600 // refresh every hour to be safe
	}
	sessKey := qrzC.SessionKey
	qrzC.Unlock()

	// Do the lookup
	url := fmt.Sprintf("https://xmldata.qrz.com/xml/current/?s=%s;callsign=%s", sessKey, call)
	resp, err := qrzHTTP.Get(url)
	if err != nil {
		w.WriteHeader(502)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var s qrzSession
	if err := xml.Unmarshal(body, &s); err != nil {
		w.WriteHeader(502)
		json.NewEncoder(w).Encode(map[string]string{"error": "QRZ XML parse: " + err.Error()})
		return
	}
	if s.Session.Error != "" {
		// Session expired or callsign not found - distinguish
		errMsg := s.Session.Error
		if strings.Contains(strings.ToLower(errMsg), "session") {
			// Force re-login next time
			qrzC.Lock()
			qrzC.SessionKey = ""
			qrzC.Unlock()
		}
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]string{"error": errMsg, "call": call})
		return
	}

	// Build response data (filter to non-sensitive fields)
	c := s.Callsign
	data := map[string]string{}
	if c.Call != "" {
		data["call"] = c.Call
	}
	if c.Fname != "" {
		data["fname"] = c.Fname
	}
	if c.Name != "" {
		data["name"] = c.Name
	}
	if c.NameFmt != "" {
		data["name_fmt"] = c.NameFmt
	}
	if c.NickN != "" {
		data["nickname"] = c.NickN
	}
	if c.Addr1 != "" {
		data["addr1"] = c.Addr1
	}
	if c.Addr2 != "" {
		data["addr2"] = c.Addr2
	}
	if c.State != "" {
		data["state"] = c.State
	}
	if c.Zip != "" {
		data["zip"] = c.Zip
	}
	if c.Country != "" {
		data["country"] = c.Country
	}
	if c.Grid != "" {
		data["grid"] = c.Grid
	}
	if c.County != "" {
		data["county"] = c.County
	}
	if c.Land != "" {
		data["land"] = c.Land
	}
	if c.Class != "" {
		data["class"] = c.Class
	}
	if c.Email != "" {
		data["email"] = c.Email
	}
	if c.URL != "" {
		data["url"] = c.URL
	}
	if c.Image != "" {
		data["image"] = c.Image
	}
	if c.Aliases != "" {
		data["aliases"] = c.Aliases
	}
	if c.Lat != "" {
		data["lat"] = c.Lat
	}
	if c.Lon != "" {
		data["lon"] = c.Lon
	}
	if c.QSLMgr != "" {
		data["qslmgr"] = c.QSLMgr
	}
	if c.EQSL != "" {
		data["eqsl"] = c.EQSL
	}
	if c.MQSL != "" {
		data["mqsl"] = c.MQSL
	}
	if c.LOTW != "" {
		data["lotw"] = c.LOTW
	}
	if c.Born != "" {
		data["born"] = c.Born
	}

	// Cache it
	qrzC.Lock()
	qrzC.Lookups[call] = qrzCachedLookup{Data: data, Fetched: time.Now().Unix()}
	// Prune old entries when cache grows large
	if len(qrzC.Lookups) > 5000 {
		cutoff := time.Now().Unix() - 86400
		for k, v := range qrzC.Lookups {
			if v.Fetched < cutoff {
				delete(qrzC.Lookups, k)
			}
		}
	}
	qrzC.Unlock()

	// Determine subscription tier - useful for UI to show 'partial data' badge
	subscriber := s.Session.SubExp != "" && s.Session.SubExp != "non-subscriber"
	msg := s.Session.Message // QRZ shows "A subscription is required" for non-subscribers

	json.NewEncoder(w).Encode(map[string]interface{}{
		"call": call, "cached": false, "data": data,
		"subscriber": subscriber,
		"message":    msg,
	})
}

// ─── Propagation Analytics ────────────────────────────────────────────────────
// Computes four analytics views from the in-memory history buffer:
//   1. Station Reliability  - A-F grade per station from packet consistency
//   2. Longest Path         - ranking of stations heard via the most digi hops
//   3. Best Time of Day     - packet count bucketed by hour (peak detection)
//   4. Activity Heatmap     - 7-day x 24-hour grid of packet counts
// All read-only, computed on demand. No persistence needed - history is the source.

func handleAnalytics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	historyMu.RLock()
	// Take a snapshot so we don't hold the lock during computation
	snap := make([]HistoryPacket, len(history))
	copy(snap, history)
	historyMu.RUnlock()

	now := time.Now()
	nowUnix := now.Unix()

	// ── 1. Station Reliability ────────────────────────────────────────────────
	// Per station: total packets, time span, average gap. A station heard
	// consistently over a long span scores high; bursty/rare ones score low.
	type staInfo struct {
		Count   int
		FirstTs int64
		LastTs  int64
		Gaps    []int64
		prevTs  int64
	}
	stations := make(map[string]*staInfo)
	for _, p := range snap {
		si := stations[p.Callsign]
		if si == nil {
			si = &staInfo{FirstTs: p.Timestamp, prevTs: p.Timestamp}
			stations[p.Callsign] = si
		}
		si.Count++
		si.LastTs = p.Timestamp
		if si.prevTs > 0 && p.Timestamp > si.prevTs {
			si.Gaps = append(si.Gaps, p.Timestamp-si.prevTs)
		}
		si.prevTs = p.Timestamp
	}

	type reliabilityRow struct {
		Callsign string `json:"callsign"`
		Grade    string `json:"grade"`
		Score    int    `json:"score"`
		Packets  int    `json:"packets"`
		SpanMin  int64  `json:"span_min"`
		AvgGapS  int64  `json:"avg_gap_s"`
	}
	var reliability []reliabilityRow
	for call, si := range stations {
		spanS := si.LastTs - si.FirstTs
		// Average gap between packets
		var avgGap int64
		if len(si.Gaps) > 0 {
			var sum int64
			for _, g := range si.Gaps {
				sum += g
			}
			avgGap = sum / int64(len(si.Gaps))
		}
		// Scoring 0-100:
		//  - packet count contributes up to 50 (log-ish: 1 pkt=5, 10=25, 50+=50)
		//  - consistency contributes up to 50 (low gap variance + decent span)
		score := 0
		if si.Count >= 50 {
			score += 50
		} else if si.Count >= 10 {
			score += 25 + (si.Count-10)*25/40
		} else {
			score += si.Count * 25 / 10
		}
		// Consistency: reward a long span with regular gaps
		if spanS > 1800 && avgGap > 0 && avgGap < 1200 {
			score += 50
		} else if spanS > 600 && avgGap > 0 && avgGap < 3600 {
			score += 30
		} else if si.Count > 1 {
			score += 15
		}
		if score > 100 {
			score = 100
		}
		grade := "F"
		switch {
		case score >= 90:
			grade = "A"
		case score >= 75:
			grade = "B"
		case score >= 60:
			grade = "C"
		case score >= 45:
			grade = "D"
		case score >= 30:
			grade = "E"
		}
		reliability = append(reliability, reliabilityRow{
			Callsign: call, Grade: grade, Score: score,
			Packets: si.Count, SpanMin: spanS / 60, AvgGapS: avgGap,
		})
	}
	// Sort by score descending
	for i := 0; i < len(reliability); i++ {
		for j := i + 1; j < len(reliability); j++ {
			if reliability[j].Score > reliability[i].Score {
				reliability[i], reliability[j] = reliability[j], reliability[i]
			}
		}
	}
	// Cap to top 50
	if len(reliability) > 50 {
		reliability = reliability[:50]
	}

	// ── 2. Longest Path (most digipeater hops) ───────────────────────────────
	// Path looks like "APRS,WIDE1-1,WIDE2-1,qAR,M0ABC". Count elements that
	// are real digipeaters (skip the destination call and q-constructs).
	type pathRow struct {
		Callsign string `json:"callsign"`
		Hops     int    `json:"hops"`
		Path     string `json:"path"`
	}
	bestPath := make(map[string]pathRow)
	for _, p := range snap {
		if p.Path == "" {
			continue
		}
		parts := strings.Split(p.Path, ",")
		hops := 0
		for i, el := range parts {
			if i == 0 {
				continue // destination callsign, not a hop
			}
			el = strings.TrimSpace(el)
			if el == "" {
				continue
			}
			// Skip q-constructs (qAR, qAO, qAS, etc) and TCPIP
			up := strings.ToUpper(el)
			if strings.HasPrefix(up, "Q") && len(up) == 3 {
				continue
			}
			if up == "TCPIP" || up == "TCPIP*" {
				continue
			}
			hops++
		}
		if hops == 0 {
			continue
		}
		existing, ok := bestPath[p.Callsign]
		if !ok || hops > existing.Hops {
			bestPath[p.Callsign] = pathRow{Callsign: p.Callsign, Hops: hops, Path: p.Path}
		}
	}
	var longestPaths []pathRow
	for _, pr := range bestPath {
		longestPaths = append(longestPaths, pr)
	}
	for i := 0; i < len(longestPaths); i++ {
		for j := i + 1; j < len(longestPaths); j++ {
			if longestPaths[j].Hops > longestPaths[i].Hops {
				longestPaths[i], longestPaths[j] = longestPaths[j], longestPaths[i]
			}
		}
	}
	if len(longestPaths) > 20 {
		longestPaths = longestPaths[:20]
	}

	// ── 3. Best Time of Day ───────────────────────────────────────────────────
	// Bucket packet timestamps by hour-of-day (server local time).
	hourCounts := make([]int, 24)
	for _, p := range snap {
		t := time.Unix(p.Timestamp, 0)
		hourCounts[t.Hour()]++
	}
	bestHour, bestHourCount := 0, 0
	for h, c := range hourCounts {
		if c > bestHourCount {
			bestHourCount = c
			bestHour = h
		}
	}

	// ── 4. Activity Heatmap (7 days x 24 hours) ──────────────────────────────
	// Grid[day][hour] = packet count. day 0 = 6 days ago, day 6 = today.
	heatmap := make([][]int, 7)
	for i := range heatmap {
		heatmap[i] = make([]int, 24)
	}
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	for _, p := range snap {
		t := time.Unix(p.Timestamp, 0)
		daysAgo := int(todayStart.Sub(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())).Hours() / 24)
		dayIdx := 6 - daysAgo
		if dayIdx >= 0 && dayIdx < 7 {
			heatmap[dayIdx][t.Hour()]++
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"generated_at":    nowUnix,
		"total_packets":   len(snap),
		"total_stations":  len(stations),
		"reliability":     reliability,
		"longest_paths":   longestPaths,
		"hour_counts":     hourCounts,
		"best_hour":       bestHour,
		"best_hour_count": bestHourCount,
		"heatmap":         heatmap,
	})
}

// ─── Met Office Severe Weather Warnings proxy ─────────────────────────────────
// The Met Office publishes UK National Severe Weather Warnings as a public
// RSS feed that requires no API key. We proxy it server-side to avoid CORS
// issues in the browser and to add a short cache. The MetOfficeKey config
// field is reserved for the optional DataHub site-specific forecast API.

var wxWarnHTTP = &http.Client{Timeout: 12 * time.Second}
var wxWarnCache struct {
	sync.RWMutex
	Body    []byte
	Fetched int64
}

func handleWxWarnings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Serve from cache if fresh (5 minutes)
	wxWarnCache.RLock()
	if wxWarnCache.Body != nil && time.Now().Unix()-wxWarnCache.Fetched < 300 {
		body := wxWarnCache.Body
		wxWarnCache.RUnlock()
		w.Write(body)
		return
	}
	wxWarnCache.RUnlock()

	// Met Office UK warnings RSS (no key needed)
	feedURL := "https://www.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/UK"
	resp, err := wxWarnHTTP.Get(feedURL)
	if err != nil {
		w.WriteHeader(502)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	// Parse the RSS - extract warning items
	type rssItem struct {
		Title       string `xml:"title"`
		Description string `xml:"description"`
		Link        string `xml:"link"`
		PubDate     string `xml:"pubDate"`
	}
	type rssFeed struct {
		Channel struct {
			Items []rssItem `xml:"item"`
		} `xml:"channel"`
	}
	var feed rssFeed
	warnings := []map[string]string{}
	if err := xml.Unmarshal(raw, &feed); err == nil {
		for _, it := range feed.Channel.Items {
			// Derive a severity from the title (Met Office uses Yellow/Amber/Red)
			level := "yellow"
			tl := strings.ToLower(it.Title)
			if strings.Contains(tl, "red warning") {
				level = "red"
			} else if strings.Contains(tl, "amber warning") {
				level = "amber"
			}
			warnings = append(warnings, map[string]string{
				"title":   it.Title,
				"desc":    it.Description,
				"link":    it.Link,
				"pubdate": it.PubDate,
				"level":   level,
			})
		}
	}

	out, _ := json.Marshal(map[string]interface{}{
		"source":   "Met Office UK National Severe Weather Warning Service",
		"fetched":  time.Now().Unix(),
		"count":    len(warnings),
		"warnings": warnings,
	})

	wxWarnCache.Lock()
	wxWarnCache.Body = out
	wxWarnCache.Fetched = time.Now().Unix()
	wxWarnCache.Unlock()

	w.Write(out)
}

func main() {
	loadOrInitCreds()
	loadSavedConfig()
	objectStore = make(map[string]*ObjectEntry)
	apiKeys = make(map[string]APIKey)
	loadWebhooksAndKeys()
	loadMemberStore()
	loadIGateHistory()
	loadBanList()
	loadMOTD()
	loadNetCheckins()
	initVAPID()
	loadPushSubs()
	go cleanExpiredSessions()

	go cleanDuplicateCache()
	go maintainUpstream()
	go handleBroadcasts()
	go listenUDP()
	go listenTCPClients()
	go keepaliveLoop()
	go runAISStream()
	go weatherBeaconLoop()
	initPerformanceMonitoring()
	go runSotaPotaFetcher()
	go runMemberObjectBeacon()
	go purgeOldHABFlights()

	http.HandleFunc("/symbols/", http.StripPrefix("/symbols/", http.FileServer(http.Dir("symbols"))).ServeHTTP)
	http.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "favicon.ico") })
	http.HandleFunc("/favicon.svg", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/svg+xml")
		http.ServeFile(w, r, "favicon.svg")
	})
	http.HandleFunc("/apple-touch-icon.png", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "apple-touch-icon.png") })
	http.HandleFunc("/favicon-32.png", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "favicon-32.png") })
	http.HandleFunc("/og-image.png", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "og-image.png") })
	http.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		http.ServeFile(w, r, "robots.txt")
	})
	http.HandleFunc("/sw.js", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/javascript")
		w.Header().Set("Service-Worker-Allowed", "/")
		http.ServeFile(w, r, "sw.js")
	})
	http.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		http.ServeFile(w, r, "sitemap.xml")
	})
	http.HandleFunc("/demo", serveDemo)
	http.HandleFunc("/mobile", serveMobile)
	http.HandleFunc("/mobile-app.js", serveMobileJS)
	http.HandleFunc("/privacy", servePrivacy)
	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/setup", handleSetup)
	http.HandleFunc("/ws", handleWS)
	http.HandleFunc("/api/config", basicAuth(handleConfig))
	http.HandleFunc("/api/config/demo", handleConfigDemo)
	http.HandleFunc("/api/history", apiKeyAuth(handleHistory))
	http.HandleFunc("/api/status", handleStatus)
	http.HandleFunc("/api/password", basicAuth(handlePassword))
	http.HandleFunc("/api/whoami", basicAuth(handleWhoami))
	http.HandleFunc("/api/objects", handleObjects)
	// Member system
	http.HandleFunc("/api/member/register", handleMemberRegister)
	http.HandleFunc("/api/member/login", handleMemberLogin)
	http.HandleFunc("/api/member/logout", handleMemberLogout)
	http.HandleFunc("/api/member/profile", handleMemberProfile)
	http.HandleFunc("/api/member/messages", handleMemberMessages)
	http.HandleFunc("/api/member/password", handleMemberPassword)
	http.HandleFunc("/api/member/object", handleMemberObject)
	http.HandleFunc("/api/member/preferences", handleMemberPreferences)
	http.HandleFunc("/api/members/callsigns", handleMembersCallsigns)
	http.HandleFunc("/api/member/message/send", handleMemberMessageSend)
	http.HandleFunc("/api/member/alert-rules", handleAlertRules)
	http.HandleFunc("/api/member/alert-rules/", handleAlertRuleDelete)
	http.HandleFunc("/api/member/wx_test", handleWxTest)
	// iGate device management (per-member, private)
	http.HandleFunc("/api/member/igates", handleMemberIGates)
	http.HandleFunc("/api/member/igate/", handleIGateCmd)
	// Push notifications
	http.HandleFunc("/api/push/vapid-key",          handlePushVapidKey)
	http.HandleFunc("/api/member/push-subscribe",   handlePushSubscribe)
	http.HandleFunc("/api/member/push-unsubscribe", handlePushUnsubscribe)
	// Telemetry dashboard
	http.HandleFunc("/api/telemetry/", handleTelemetry)
	// SOTA / POTA live spots + map overlay
	http.HandleFunc("/api/sota-pota", handleSotaPota)
	// HAB flight tracker
	http.HandleFunc("/api/hab",  handleHAB)
	http.HandleFunc("/api/hab/", handleHAB)
	// Member APRS objects
	http.HandleFunc("/api/member/objects",  handleMemberObjects)
	http.HandleFunc("/api/member/objects/", handleMemberObjects)

	// Server federation registry
	http.HandleFunc("/api/server/register", handleServerRegister)
	http.HandleFunc("/api/admin/servers", basicAuth(handleAdminServers))
	// Public MOTD
	http.HandleFunc("/api/motd", handlePublicMOTD)
	// Admin features
	http.HandleFunc("/api/admin/members", basicAuth(handleAdminMembers))
	http.HandleFunc("/api/admin/members/", basicAuth(handleAdminMemberRouter))
	http.HandleFunc("/api/admin/bans", basicAuth(handleAdminBans))
	http.HandleFunc("/api/admin/motd", basicAuth(handleAdminMOTD))
	http.HandleFunc("/api/admin/audit", basicAuth(handleAdminAudit))
	http.HandleFunc("/api/admin/backup", basicAuth(handleAdminBackup))
	http.HandleFunc("/api/admin/restore", basicAuth(handleAdminRestore))
	http.HandleFunc("/api/admin/update", basicAuth(handleAdminUpdate))
	http.HandleFunc("/api/admin/performance", basicAuth(handleAdminPerformance))
	http.HandleFunc("/metrics", basicAuth(handleMetrics))
	http.HandleFunc("/api/webhooks", basicAuth(handleWebhooks))
	http.HandleFunc("/api/webhooks/", basicAuth(handleWebhookDelete))
	http.HandleFunc("/api/keys", basicAuth(handleAPIKeys))
	http.HandleFunc("/api/keys/", basicAuth(handleAPIKeyDelete))
	http.HandleFunc("/api/export/geojson", handleGeoJSON)
	http.HandleFunc("/api/export/kml", handleKML)
	http.HandleFunc("/api/tle", handleTLE)
	http.HandleFunc("/api/ariss", handleARISS)
	http.HandleFunc("/api/version", handleVersion)
	http.HandleFunc("/api/firmware-proxy", handleFirmwareProxy)
	http.HandleFunc("/api/tocalls", handleTocalls)
	http.HandleFunc("/api/qrz/lookup", handleQRZLookup)
	http.HandleFunc("/api/analytics", handleAnalytics)
	http.HandleFunc("/api/wx/warnings", handleWxWarnings)
	http.HandleFunc("/api/messages", handleMessages)
	http.HandleFunc("/api/iss", handleISSPosition)
	http.HandleFunc("/api/update", basicAuth(handleUpdate))
	http.HandleFunc("/api/netcheckins/stats", handleNetCheckinStats)
	http.HandleFunc("/api/netcheckins/leaderboard", handleNetCheckinLeaderboard)
	http.HandleFunc("/api/netcheckins/me", handleNetCheckinMe)
	http.HandleFunc("/api/netcheckins/archive", handleNetCheckinArchive)

	log.Printf("Advanced APRS Gateway active on :8080")
	go startIGateMQTTBroker() // per-member iGate management (TCP :1883)
	server := &http.Server{
		Addr:              ":8080",
		Handler:           nil,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	log.Fatal(server.ListenAndServe())
}

// serveMobile serves the touch-optimised mobile companion page.
func serveMobile(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "mobile.html")
}

// servePrivacy serves the privacy policy (required for Play Store /
// App Store listings and general transparency).
func servePrivacy(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "privacy.html")
}

// serveMobileJS serves the mobile companion page's JavaScript.
func serveMobileJS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript")
	http.ServeFile(w, r, "mobile-app.js")
}

// serveIndex redirects to /setup if no credentials have been configured yet.
func serveIndex(w http.ResponseWriter, r *http.Request) {
	// Serve Google Search Console HTML verification files transparently.
	// Drop the file (e.g. google1a2b3c4d.html) in /opt/aprs-gateway/ and
	// it will be accessible at https://www.aprsnet.uk/google1a2b3c4d.html
	// without rebuilding or restarting the server.
	if path := r.URL.Path; len(path) > 7 &&
		strings.HasPrefix(path, "/google") &&
		strings.HasSuffix(path, ".html") {
		http.ServeFile(w, r, path[1:]) // strip leading /
		return
	}
	if !credsConfigured() {
		http.Redirect(w, r, "/setup", http.StatusFound)
		return
	}
	http.ServeFile(w, r, "index.html")
}

// serveDemo serves the dashboard in read-only demo mode.
// Sets a cookie so the frontend knows to hide all write controls.
func serveDemo(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     "aprs_demo",
		Value:    "1",
		Path:     "/",
		MaxAge:   86400,
		HttpOnly: false,
		SameSite: http.SameSiteLaxMode,
	})
	http.ServeFile(w, r, "index.html")
}

func basicAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		adminCreds.RLock()
		wantUser, wantPass := adminCreds.Username, adminCreds.Password
		adminCreds.RUnlock()
		if !ok || user != wantUser || pass != wantPass {
			// Do NOT send WWW-Authenticate — that triggers the browser native dialog.
			// Our frontend handles auth with a custom login gate.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		next(w, r)
	}
}

// handlePassword is defined below in the first-run setup section.

func verifyPasscode(callsign, passcode string) bool {
	if passcode == "-1" {
		return false
	}
	baseCall := strings.Split(strings.ToUpper(callsign), "-")[0]
	hash := 0x73e2
	for i := 0; i < len(baseCall); i += 2 {
		hash ^= int(baseCall[i]) << 8
		if i+1 < len(baseCall) {
			hash ^= int(baseCall[i+1])
		}
	}
	return fmt.Sprintf("%d", hash&0x7fff) == passcode
}

func injectQConstruct(packet, qType string) string {
	config.RLock()
	srv := config.ServerName
	config.RUnlock()
	gtIdx := strings.Index(packet, ">")
	if gtIdx == -1 {
		return packet
	}
	colIdx := strings.Index(packet[gtIdx:], ":")
	if colIdx == -1 {
		return packet
	}
	colIdx += gtIdx
	header := packet[:colIdx]
	if strings.Contains(header, ",qA") {
		return packet
	}
	return fmt.Sprintf("%s,%s,%s%s", header, qType, srv, packet[colIdx:])
}

func isDuplicate(packet string) bool {
	hashData := packet
	gtIdx := strings.Index(packet, ">")
	colIdx := strings.Index(packet, ":")
	if gtIdx != -1 && colIdx != -1 && gtIdx < colIdx {
		header := packet[:colIdx]
		if qIdx := strings.Index(header, ",qA"); qIdx != -1 {
			hashData = header[:qIdx] + packet[colIdx:]
		}
	}
	dupesMu.Lock()
	defer dupesMu.Unlock()
	if _, exists := dupes[hashData]; exists {
		return true
	}
	dupes[hashData] = time.Now()
	return false
}

func cleanDuplicateCache() {
	for {
		time.Sleep(1 * time.Minute)
		now := time.Now()
		dupesMu.Lock()
		for k, v := range dupes {
			if now.Sub(v) > 5*time.Minute {
				delete(dupes, k)
			}
		}
		dupesMu.Unlock()
	}
}

func maintainUpstream() {
	for {
		connectUpstream()
		atomic.StoreInt32(&upstreamConnected, 0)
		time.Sleep(5 * time.Second)
	}
}

func connectUpstream() {
	config.RLock()
	addr, call, pass, filter, vers := config.UpstreamAddr, config.Callsign, config.Passcode, config.ServerFilter, config.SoftwareVers
	cLat, cLon, rad := config.CenterLat, config.CenterLon, config.RadiusKm
	config.RUnlock()

	filter = strings.TrimSpace(filter)
	if strings.ToLower(filter) == "auto" || filter == "" {
		filter = fmt.Sprintf("r/%.4f/%.4f/%.0f", cLat, cLon, rad)
	}

	// Defensive: drop any 't/' or '-t/' clauses with zero type letters.
	// APRS-IS spec: an empty t/ means "pass nothing" - which is almost never
	// what the admin intended. The admin UI now also strips these on the
	// client side, but we re-check here in case an older config or a manual
	// edit slipped one through.
	if strings.Contains(filter, "t/") {
		parts := strings.Fields(filter)
		cleaned := make([]string, 0, len(parts))
		for _, p := range parts {
			lp := strings.TrimPrefix(p, "-")
			if strings.ToLower(lp) == "t/" {
				continue
			}
			cleaned = append(cleaned, p)
		}
		filter = strings.Join(cleaned, " ")
		if filter == "" {
			filter = fmt.Sprintf("r/%.4f/%.4f/%.0f", cLat, cLon, rad)
		}
	}

	conn, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		log.Printf("Upstream connect failed (%s): %v", addr, err)
		return
	}
	defer conn.Close()

	loginLine := fmt.Sprintf("user %s pass %s vers %s filter %s\r\n", call, pass, vers, filter)
	log.Printf("APRS-IS login: user=%s filter=%s", call, filter)
	if _, err := fmt.Fprint(conn, loginLine); err != nil {
		return
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	scanner := bufio.NewScanner(conn)
	verified := false
	for scanner.Scan() {
		line := scanner.Text()
		log.Printf("APRS-IS banner: %s", line)
		if strings.Contains(line, "logresp") {
			if strings.Contains(line, "verified") && !strings.Contains(line, "unverified") {
				verified = true
			}
			break
		}
	}
	conn.SetReadDeadline(time.Time{})

	if !verified {
		log.Printf("APRS-IS login not verified for %s — check callsign/passcode", call)
	}
	atomic.StoreInt32(&upstreamConnected, 1)
	log.Printf("Connected to APRS-IS upstream: %s (verified=%v)", addr, verified)

	// Goroutine to forward any pending TX packets (e.g. member-created objects)
	txDone := make(chan struct{})
	go func() {
		defer close(txDone)
		for {
			select {
			case pkt := <-upstreamTx:
				if _, err := fmt.Fprintf(conn, "%s\r\n", pkt); err != nil {
					log.Printf("Upstream TX failed: %v", err)
					return
				}
				log.Printf("Upstream TX: %s", pkt)
			case <-time.After(1 * time.Hour):
				return // safety exit
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for scanner.Scan() {
			text := scanner.Text()
			if strings.HasPrefix(text, "#") {
				log.Printf("APRS-IS: %s", text)
				continue
			}
			metrics.Lock()
			metrics.PktsRx++
			metrics.BytesRx += uint64(len(text))
			metrics.Unlock()
			if isAllowed(text) && !isDuplicate(text) {
				broadcast <- text
			} else {
				metrics.Lock()
				metrics.PktsDropped++
				metrics.Unlock()
			}

		}
	}()

	for {
		select {
		case <-done:
			atomic.StoreInt32(&upstreamConnected, 0)
			return
		case <-reconnectChan:
			return
		case pkt := <-upstreamOut:
			outBytes := []byte(pkt + "\r\n")
			conn.Write(outBytes)
			metrics.Lock()
			metrics.PktsTx++
			metrics.BytesTx += uint64(len(outBytes))
			metrics.Unlock()
		}
	}
}

func isAllowed(packet string) bool {
	gtIdx := strings.Index(packet, ">")
	colIdx := strings.Index(packet, ":")
	if gtIdx == -1 || colIdx == -1 || gtIdx > colIdx {
		return false
	}
	// APRS message packets have format `SRC>...:`+`:DEST_____:body` (a second
	// colon followed by 9 chars then another colon). These are one-to-one
	// human messaging traffic (including ACKs) and must NEVER be silenced by
	// the drop filters - those exist to hide noisy beacons, not to break
	// messaging. Without this exemption, an ACK from a recipient whose
	// network path happens to include a DV-gateway tocall (APDG??, APIRCD,
	// IRCDDB) was being dropped, leaving outbound messages stuck in SENT
	// state until the app gave up and marked them FAILED.
	isMessage := false
	if len(packet) > colIdx+11 && packet[colIdx+1] == ':' && packet[colIdx+11] == ':' {
		isMessage = true
	}
	upper := strings.ToUpper(packet)
	config.RLock()
	dPi, dD, dDesk, geo, cLat, cLon, rad := config.DropPiStar, config.DropDStar, config.DropAPDesk, config.EnableGeofence, config.CenterLat, config.CenterLon, config.RadiusKm
	config.RUnlock()
	if !isMessage {
		if dPi && (strings.Contains(upper, "PISTAR") || strings.Contains(upper, "MMDVM") || strings.Contains(upper, "APDPRS") || strings.Contains(upper, "APDG") || strings.Contains(upper, "APIRCD") || strings.Contains(upper, "IRCDDB")) {
			return false
		}
		if dD && (strings.Contains(upper, "D-STAR") || strings.Contains(upper, "APDSTR")) {
			return false
		}
		if dDesk && strings.Contains(upper, "APDESK") {
			return false
		}
	}
	if geo {
		if parsed, ok := parsePacket(packet); ok {
			dLat := (parsed.Lat - cLat) * 111.0
			dLon := (parsed.Lon - cLon) * 111.0 * math.Cos(cLat*math.Pi/180)
			if (dLat*dLat + dLon*dLon) > (rad * rad) {
				return false
			}
		}
	}
	return true
}

func parsePacket(packet string) (HistoryPacket, bool) {
	var hp HistoryPacket
	gtIdx := strings.Index(packet, ">")
	colIdx := strings.Index(packet, ":")
	if gtIdx == -1 || colIdx == -1 || gtIdx > colIdx {
		return hp, false
	}
	hp.Callsign = packet[:gtIdx]
	hp.Path = packet[gtIdx+1 : colIdx]
	hp.Raw = packet
	hp.Timestamp = time.Now().Unix()
	payload := packet[colIdx+1:]

	// ── Uncompressed position ─────────────────────────────────────────────────
	match := posRegex.FindStringSubmatch(payload)
	if match != nil {
		lDeg, _ := strconv.ParseFloat(match[1], 64)
		lMin, _ := strconv.ParseFloat(match[2], 64)
		hp.Lat = lDeg + lMin/60
		if match[3] == "S" {
			hp.Lat = -hp.Lat
		}
		lnDeg, _ := strconv.ParseFloat(match[5], 64)
		lnMin, _ := strconv.ParseFloat(match[6], 64)
		hp.Lon = lnDeg + lnMin/60
		if match[7] == "W" {
			hp.Lon = -hp.Lon
		}
		hp.Symbol = match[4] + match[8]
		if pMatch := phgRegex.FindStringSubmatch(payload); pMatch != nil {
			hp.PHG = pMatch[0]
		}
		return hp, true
	}

	// ── Compressed position (Base91) ──────────────────────────────────────────
	// Used by OE5BPA firmware, LoRa_APRS_Tracker, and most ESP32 LoRa nodes.
	// Format: DTI sym_table(1) latB91(4) lonB91(4) symbol(1) cs(2) T(1)
	if cm := compPosRegex.FindStringSubmatch(payload); cm != nil {
		symTable := cm[1] // "/" or "\"
		latB91 := cm[2]
		lonB91 := cm[3]
		symCode := cm[4]
		// Validate: all chars must be in printable ASCII 33–123
		valid := true
		for _, s := range []string{latB91, lonB91} {
			for _, c := range s {
				if c < 33 || c > 123 {
					valid = false
					break
				}
			}
		}
		if valid {
			hp.Lat = 90.0 - float64(decodeBase91Pos(latB91))/380926.0
			hp.Lon = -180.0 + float64(decodeBase91Pos(lonB91))/190463.0
			// Sanity-check decoded coordinates
			if hp.Lat >= -90 && hp.Lat <= 90 && hp.Lon >= -180 && hp.Lon <= 180 {
				hp.Symbol = symTable + symCode
				return hp, true
			}
		}
	}

	return hp, false
}

// parseAPRSMessage extracts an APRS message packet into a MsgEntry.
// Returns nil if the packet is not a message.
func parseAPRSMessage(packet string) *MsgEntry {
	gtIdx := strings.Index(packet, ">")
	colIdx := strings.Index(packet, ":")
	if gtIdx == -1 || colIdx == -1 || gtIdx > colIdx {
		return nil
	}
	from := packet[:gtIdx]
	payload := packet[colIdx+1:]
	// APRS message format: :CALLSIGN :text{id}
	if len(payload) < 11 || payload[0] != ':' {
		return nil
	}
	to := strings.TrimSpace(payload[1:10])
	body := payload[10:]
	if len(body) < 1 || body[0] != ':' {
		return nil
	}
	body = body[1:]
	msgID := ""
	if idx := strings.LastIndex(body, "{"); idx != -1 {
		msgID = body[idx+1:]
		body = body[:idx]
		msgID = strings.TrimRight(msgID, "}")
	}
	// Skip acks
	if strings.HasPrefix(strings.ToLower(body), "ack") {
		return nil
	}
	return &MsgEntry{
		Timestamp: time.Now().Unix(),
		From:      from,
		To:        to,
		Text:      strings.TrimSpace(body),
		MsgID:     msgID,
		Packet:    packet,
	}
}

// ─── Object / Item parsing ────────────────────────────────────────────────────

func parseObject(packet string) (*ObjectEntry, bool) {
	gtIdx := strings.Index(packet, ">")
	colIdx := strings.Index(packet, ":")
	if gtIdx == -1 || colIdx == -1 || gtIdx > colIdx {
		return nil, false
	}
	owner := packet[:gtIdx]
	payload := packet[colIdx+1:]

	// Try object format
	if m := objRegex.FindStringSubmatch(payload); m != nil {
		name := strings.TrimRight(m[1], " ")
		killed := strings.Contains(packet, "_") && !strings.Contains(packet, "*")
		lDeg, _ := strconv.ParseFloat(m[3], 64)
		lMin, _ := strconv.ParseFloat(m[4], 64)
		lat := lDeg + lMin/60
		if m[5] == "S" {
			lat = -lat
		}
		lnDeg, _ := strconv.ParseFloat(m[7], 64)
		lnMin, _ := strconv.ParseFloat(m[8], 64)
		lon := lnDeg + lnMin/60
		if m[9] == "W" {
			lon = -lon
		}
		sym := m[6] + m[10]
		comment := ""
		if len(payload) > len(m[0]) {
			comment = strings.TrimSpace(payload[strings.Index(payload, m[0])+len(m[0]):])
		}
		return &ObjectEntry{
			Timestamp: time.Now().Unix(),
			Name:      name, Owner: owner,
			Lat: lat, Lon: lon,
			Symbol: sym, Comment: comment,
			Killed: killed, Raw: packet,
		}, true
	}

	// Try item format
	if m := itemRegex.FindStringSubmatch(payload); m != nil {
		name := strings.TrimRight(m[1], " ")
		killed := payload[len(name)+1] == '_'
		lDeg, _ := strconv.ParseFloat(m[2], 64)
		lMin, _ := strconv.ParseFloat(m[3], 64)
		lat := lDeg + lMin/60
		if m[4] == "S" {
			lat = -lat
		}
		lnDeg, _ := strconv.ParseFloat(m[6], 64)
		lnMin, _ := strconv.ParseFloat(m[7], 64)
		lon := lnDeg + lnMin/60
		if m[8] == "W" {
			lon = -lon
		}
		sym := m[5] + m[9]
		return &ObjectEntry{
			Timestamp: time.Now().Unix(),
			Name:      name, Owner: owner,
			Lat: lat, Lon: lon,
			Symbol: sym, Killed: killed, Raw: packet,
		}, true
	}
	return nil, false
}

// handleObjects returns all active (non-killed) objects and items.
func handleObjects(w http.ResponseWriter, r *http.Request) {
	objectStoreMu.RLock()
	defer objectStoreMu.RUnlock()
	result := make([]*ObjectEntry, 0, len(objectStore))
	for _, o := range objectStore {
		if !o.Killed {
			result = append(result, o)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleBroadcasts() {
	for packet := range broadcast {
		parsed, hasCoords := parsePacket(packet)
		if hasCoords {
			historyMu.Lock()
			history = append(history, parsed)
			if len(history) > maxHistory {
				history = history[1:]
			}
			historyMu.Unlock()
		}

		// Increment metrics
		atomic.AddInt64(&totalPackets, 1)
		atomic.AddInt64(&upstreamBytes, int64(len(packet)))

		// Fire webhooks for position packets + check geofence alert rules
		if hasCoords {
			go fireWebhooks("position", parsed.Callsign, parsed)
			go checkGeofenceAlerts(parsed.Callsign, parsed.Lat, parsed.Lon)
			// HAB flight tracker: check every position packet for balloon signature
			go ingestHABPacket(parsed.Callsign, parsed.Lat, parsed.Lon, parsed.Raw)
		}
		// APRS telemetry packets (T#, PARM, UNIT, EQNS)
		go ingestTelemPacket(packet)

		// Store in raw ring buffer (trim entries older than 1 hour)
		now := time.Now().Unix()
		rawRingMu.Lock()
		rawRing = append(rawRing, RawEntry{Timestamp: now, Packet: packet})
		for len(rawRing) > 0 && now-rawRing[0].Timestamp > 3600 {
			rawRing = rawRing[1:]
		}
		rawRingMu.Unlock()

		// Parse and store APRS objects/items
		var objHP *HistoryPacket
		if obj, ok := parseObject(packet); ok {
			objectStoreMu.Lock()
			if obj.Killed {
				delete(objectStore, obj.Name)
			} else {
				objectStore[obj.Name] = obj
			}
			objectStoreMu.Unlock()
			// Stash parsed coords so we can include them in the WS message below
			if !obj.Killed && !hasCoords {
				hp := HistoryPacket{
					Callsign:  obj.Name,
					Timestamp: obj.Timestamp,
					Lat:       obj.Lat,
					Lon:       obj.Lon,
					Symbol:    obj.Symbol,
					Raw:       packet,
				}
				objHP = &hp
			}
		}

		// Parse and store APRS messages
		// Check ban list - drop packets from banned callsigns
		if gtIdx := strings.Index(packet, ">"); gtIdx > 0 {
			src := packet[:gtIdx]
			if reason := isCallsignBanned(src); reason != "" {
				continue
			}
		}
		if msg := parseAPRSMessage(packet); msg != nil {
			go fireWebhooks("message", msg.From, msg)
			go storeMessageForMember(msg.To, msg.From, msg.Text, packet, time.Now().Unix())
			msgStoreMu.Lock()
			msgStore = append(msgStore, *msg)
			for len(msgStore) > 0 && now-msgStore[0].Timestamp > 3600 {
				msgStore = msgStore[1:]
			}
			msgStoreMu.Unlock()
			// Net check-in detection: matches msg.To against the known
			// net destinations (ANSRVR, APRSPH, 9M4GKS) — see
			// net_checkins.go. TOCALL is the destination field right
			// after '>' in FROM>TOCALL,PATH:..., used to identify the
			// sender's software via the aprs-deviceid database.
			tocall := ""
			if gt := strings.Index(packet, ">"); gt > 0 {
				rest := packet[gt+1:]
				end := strings.IndexAny(rest, ",:")
				if end > 0 {
					tocall = rest[:end]
				}
			}
			go recordNetCheckinIfMatch(msg.From, msg.To, msg.Text, tocall)
		}
		msg := wsMessage{Type: "rx", Packet: packet}
		if hasCoords {
			msg.Data = parsed
		} else if objHP != nil {
			msg.Data = *objHP
		}
		// Check if this is an object/item and tag it
		payload := ""
		if ci := strings.Index(packet, ":"); ci >= 0 {
			payload = packet[ci+1:]
		}
		if len(payload) > 0 && (payload[0] == ';' || payload[0] == ')') {
			msg.Type = "obj"
		}
		data, _ := json.Marshal(msg)
		clientsMu.Lock()
		for c := range clients {
			select {
			case c.send <- data:
			default:
			}
		}
		clientsMu.Unlock()
		// Also push to TCP APRS-IS clients
		broadcastToTCPClients(packet)
	}
}

// WebSocket keepalive: without this, a client whose TCP connection drops
// silently (laptop sleeps, WiFi drops, NAT/mobile timeout — no FIN/RST ever
// received) leaves ReadMessage() blocking forever with no deadline, so the
// deferred cleanup in handleWS never runs and the dead session lingers in
// the clients map / connected-clients dashboard indefinitely (shown as
// still "Auth"'d, e.g. a stale desktop-app entry with nobody connected).
const (
	wsPongWait   = 60 * time.Second
	wsPingPeriod = (wsPongWait * 9) / 10 // must be < wsPongWait
)

func (c *wsClient) writePump() {
	ticker := time.NewTicker(wsPingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case data, ok := <-c.send:
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		case <-ticker.C:
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	if !ipConnAllow(getRealIP(r)) {
		conn.Close()
		return
	}
	defer ipConnRelease(getRealIP(r))
	client := &wsClient{
		conn:        conn,
		send:        make(chan []byte, 1024),
		remoteAddr:  getRealIP(r),
		connectedAt: time.Now().Unix(),
		lastTx:      time.Now(),
	}
	clientsMu.Lock()
	clients[client] = true
	clientsMu.Unlock()
	// Read-side of the keepalive: extend the deadline on every pong (and
	// every ordinary message), so a genuinely dead connection — one that
	// stops responding to pings — gets its ReadMessage() call unblocked by
	// the deadline instead of hanging forever, letting the deferred
	// cleanup below actually run and remove the stale client.
	conn.SetReadDeadline(time.Now().Add(wsPongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(wsPongWait))
		return nil
	})
	go client.writePump()

	// Send replay of last hour to new client
	go func() {
		// Small delay so client JS is ready
		time.Sleep(300 * time.Millisecond)

		// Raw packet replay
		rawRingMu.RLock()
		replay := make([]RawEntry, len(rawRing))
		copy(replay, rawRing)
		rawRingMu.RUnlock()

		for _, e := range replay {
			msg := wsMessage{Type: "rx", Packet: e.Packet}
			if parsed, ok := parsePacket(e.Packet); ok {
				msg.Data = parsed
			}
			data, _ := json.Marshal(msg)
			select {
			case client.send <- data:
			default:
			}
		}

		// Message replay
		msgStoreMu.RLock()
		msgs := make([]MsgEntry, len(msgStore))
		copy(msgs, msgStore)
		msgStoreMu.RUnlock()

		for _, m := range msgs {
			data, _ := json.Marshal(wsMessage{Type: "msg_history", Packet: m.Packet,
				Callsign: m.From, Data: m})
			select {
			case client.send <- data:
			default:
			}
		}

		// Signal end of replay
		data, _ := json.Marshal(wsMessage{Type: "replay_done"})
		select {
		case client.send <- data:
		default:
		}
	}()

	defer func() {
		clientsMu.Lock()
		delete(clients, client)
		clientsMu.Unlock()
		close(client.send)
	}()
	for {
		_, msgData, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var in wsMessage
		if err := json.Unmarshal(msgData, &in); err != nil {
			continue
		}
		switch in.Type {
		case "replay_request":
			// Client wants historical packets - send up to in.Minutes minutes back
			minutes := in.Minutes
			if minutes <= 0 {
				minutes = 60
			}
			if minutes > 120 {
				minutes = 120
			}
			go sendReplayToClient(client, minutes)
		case "auth":
			if verifyPasscode(in.Callsign, in.Passcode) {
				stale := registerWSAuthentication(client, in.Callsign, in.Software, in.ClientID)
				closeStaleWSClients(stale)
				ack, _ := json.Marshal(wsMessage{Type: "auth_ack", Status: "success", Callsign: client.callsign})
				client.send <- ack
				// Deliver any stored offline messages to this callsign
				go func(c *wsClient, call string) {
					time.Sleep(500 * time.Millisecond) // small delay so client can finish setup
					forwardOfflineMessagesToCallsign(call, func(pkt string) {
						msg := wsMessage{Type: "rx", Packet: pkt}
						if parsed, ok := parsePacket(pkt); ok {
							msg.Data = parsed
						}
						data, _ := json.Marshal(msg)
						select {
						case c.send <- data:
						case <-time.After(1 * time.Second):
						}
					})
				}(client, client.callsign)
			} else {
				ack, _ := json.Marshal(wsMessage{Type: "auth_ack", Status: "fail"})
				client.send <- ack
			}
		case "tx":
			if !client.authenticated {
				continue
			}
			// The app authenticates with the BARE callsign (e.g. 2E0LXY) but
			// transmits FROM a callsign-with-SSID (e.g. 2E0LXY-9>APRS,...).
			// Previously this check was `HasPrefix(packet, callsign+">")`
			// which rejected every SSIDed source - silently dropping all
			// app messages and ACKs. Parse the source field properly and
			// compare against the bare callsign only.
			gt := strings.Index(in.Packet, ">")
			if gt == -1 {
				log.Printf("WS TX rejected (no '>' in packet) from %s: %q", client.callsign, in.Packet)
				continue
			}
			src := strings.ToUpper(in.Packet[:gt])
			if dash := strings.Index(src, "-"); dash != -1 {
				src = src[:dash]
			}
			if src != client.callsign {
				log.Printf("WS TX rejected (source %q != auth callsign %q): %q", src, client.callsign, in.Packet)
				continue
			}
			if time.Since(client.lastTx) < 5*time.Second {
				log.Printf("WS TX rate-limited from %s", client.callsign)
				continue
			}
			client.lastTx = time.Now()
			metrics.Lock()
			metrics.PktsRx++
			metrics.BytesRx += uint64(len(in.Packet))
			metrics.Unlock()
			routed := injectQConstruct(in.Packet, "qAC")
			if isAllowed(routed) {
				// Authenticated WS clients are rate-limited to 1/s above.
				// isDuplicate() uses a 5-minute window which wrongly blocks
				// stationary beacons (same packet string every interval).
				// Skip dedup here — the rate-limit is the right gate for
				// WS TX; dedup exists for IGate-injected packets.
				log.Printf("WS TX %s: %s", client.callsign, routed)
				broadcast <- routed
				upstreamOut <- routed
			} else {
				log.Printf("WS TX dropped by isAllowed filter (%s): %s", client.callsign, routed)
				metrics.Lock()
				metrics.PktsDropped++
				metrics.Unlock()
			}
		}
	}
}

// sendReplayToClient streams the last N minutes of packets to a single client
// in response to a replay_request message. Non-blocking on the send channel.
func sendReplayToClient(client *wsClient, minutes int) {
	cutoff := time.Now().Add(-time.Duration(minutes) * time.Minute).Unix()

	// Raw packet replay - filter by timestamp
	rawRingMu.RLock()
	replay := make([]RawEntry, 0, len(rawRing))
	for _, e := range rawRing {
		if e.Timestamp >= cutoff {
			replay = append(replay, e)
		}
	}
	rawRingMu.RUnlock()

	// Tell client how many to expect
	startData, _ := json.Marshal(wsMessage{
		Type: "replay_start",
		Data: map[string]int{"total": len(replay), "minutes": minutes},
	})
	select {
	case client.send <- startData:
	default:
	}

	for _, e := range replay {
		msg := wsMessage{Type: "rx", Packet: e.Packet}
		if parsed, ok := parsePacket(e.Packet); ok {
			msg.Data = parsed
		}
		data, _ := json.Marshal(msg)
		select {
		case client.send <- data:
		case <-time.After(2 * time.Second):
			// Client too slow - abort this replay
			return
		}
	}

	// Message replay (also filtered)
	msgStoreMu.RLock()
	msgs := make([]MsgEntry, 0, len(msgStore))
	for _, m := range msgStore {
		if m.Timestamp >= cutoff {
			msgs = append(msgs, m)
		}
	}
	msgStoreMu.RUnlock()

	for _, m := range msgs {
		data, _ := json.Marshal(wsMessage{Type: "msg_history", Packet: m.Packet,
			Callsign: m.From, Data: m})
		select {
		case client.send <- data:
		default:
		}
	}

	// Signal end of replay
	doneData, _ := json.Marshal(wsMessage{Type: "replay_done",
		Data: map[string]int{"packets": len(replay)}})
	select {
	case client.send <- doneData:
	default:
	}
}

func listenUDP() {
	addr, err := net.ResolveUDPAddr("udp", ":14580")
	if err != nil {
		log.Printf("UDP resolve failed: %v", err)
		return
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Printf("UDP listen failed (port 14580): %v", err)
		return
	}
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		p := strings.TrimSpace(string(buf[:n]))
		if p == "" || strings.HasPrefix(p, "#") {
			continue
		}
		metrics.Lock()
		metrics.PktsRx++
		metrics.BytesRx += uint64(n)
		metrics.Unlock()
		routed := injectQConstruct(p, "qAU")
		if isAllowed(routed) && !isDuplicate(routed) {
			broadcast <- routed
			upstreamOut <- routed
		} else {
			metrics.Lock()
			metrics.PktsDropped++
			metrics.Unlock()
		}
	}
}

// ─── TCP APRS-IS Client Listener (port 14580) ────────────────────────────────
// Allows standard APRS clients (APRSDroid, Xastir, YAAC, Direwolf) to connect
// directly to this server using the standard APRS-IS protocol.

type tcpClient struct {
	conn        net.Conn
	callsign    string
	verified    bool
	filter      string
	remoteAddr  string
	connectedAt int64
}

var (
	tcpClients   = make(map[*tcpClient]bool)
	tcpClientsMu sync.Mutex
)

func listenTCPClients() {
	ln, err := net.Listen("tcp", ":14580")
	if err != nil {
		log.Printf("TCP client listener failed: %v", err)
		return
	}
	defer ln.Close()
	log.Printf("TCP APRS-IS client listener active on :14580")
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleTCPClient(conn)
	}
}

func handleTCPClient(conn net.Conn) {
	defer conn.Close()
	if !ipConnAllow(conn.RemoteAddr().String()) {
		fmt.Fprintf(conn, "# Too many connections from your address\r\n")
		return
	}
	defer ipConnRelease(conn.RemoteAddr().String())
	client := &tcpClient{
		conn:        conn,
		remoteAddr:  conn.RemoteAddr().String(),
		connectedAt: time.Now().Unix(),
	}

	config.RLock()
	srvName := config.ServerName
	srvVers := config.SoftwareVers
	config.RUnlock()

	// Send server banner
	fmt.Fprintf(conn, "# %s %s\r\n", srvName, srvVers)

	tcpClientsMu.Lock()
	tcpClients[client] = true
	tcpClientsMu.Unlock()
	defer func() {
		tcpClientsMu.Lock()
		delete(tcpClients, client)
		tcpClientsMu.Unlock()
		if client.callsign != "" {
			log.Printf("TCP client disconnected: %s (%s)", client.callsign, client.remoteAddr)
		}
	}()

	scanner := bufio.NewScanner(conn)
	loggedIn := false

	for scanner.Scan() {
		line := sanitisePacket(strings.TrimSpace(scanner.Text()))
		if line == "" {
			continue
		}

		// Login line: user CALLSIGN pass PASSCODE vers SOFTWARE filter FILTER
		if !loggedIn && strings.HasPrefix(strings.ToLower(line), "user ") {
			parts := strings.Fields(line)
			call, pass := "", ""
			for i, p := range parts {
				if strings.ToLower(p) == "user" && i+1 < len(parts) {
					call = strings.ToUpper(parts[i+1])
				}
				if strings.ToLower(p) == "pass" && i+1 < len(parts) {
					pass = parts[i+1]
				}
				if strings.ToLower(p) == "filter" && i+1 < len(parts) {
					client.filter = strings.Join(parts[i+1:], " ")
				}
			}
			client.callsign = call
			client.verified = verifyPasscode(call, pass)

			status := "unverified"
			if client.verified {
				status = "verified"
			}
			config.RLock()
			srv := config.ServerName
			config.RUnlock()
			fmt.Fprintf(conn, "# logresp %s %s, server %s\r\n", call, status, srv)
			log.Printf("TCP client login: %s %s from %s", call, status, client.remoteAddr)
			loggedIn = true

			// Deliver any stored offline messages to this callsign
			if client.verified {
				go func(c net.Conn, call string) {
					time.Sleep(500 * time.Millisecond)
					forwardOfflineMessagesToCallsign(call, func(pkt string) {
						_, _ = fmt.Fprintf(c, "%s\r\n", pkt)
					})
				}(conn, call)
			}

			metrics.Lock()
			metrics.PktsRx++
			metrics.Unlock()
			continue
		}

		if !loggedIn {
			continue
		}

		// Ignore comment lines from client
		if strings.HasPrefix(line, "#") {
			continue
		}

		// Incoming packet from client — must start with their callsign
		if !strings.HasPrefix(strings.ToUpper(line), client.callsign+">") {
			continue
		}
		if !client.verified {
			// Read-only clients cannot inject packets
			continue
		}

		metrics.Lock()
		metrics.PktsRx++
		metrics.BytesRx += uint64(len(line))
		metrics.Unlock()

		routed := injectQConstruct(line, "qAC")
		if isAllowed(routed) && !isDuplicate(routed) {
			broadcast <- routed
			upstreamOut <- routed
		} else {
			metrics.Lock()
			metrics.PktsDropped++
			metrics.Unlock()
		}
	}
}

// broadcastToTCPClients sends a packet to all connected TCP clients.
// Called from handleBroadcasts.
func broadcastToTCPClients(packet string) {
	line := packet + "\r\n"
	// Snapshot the client list so we don't hold the mutex during writes
	tcpClientsMu.Lock()
	snap := make([]*tcpClient, 0, len(tcpClients))
	for c := range tcpClients {
		if c.callsign != "" {
			snap = append(snap, c)
		}
	}
	tcpClientsMu.Unlock()
	for _, c := range snap {
		go func(cl *tcpClient, l string) {
			cl.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err := fmt.Fprint(cl.conn, l)
			cl.conn.SetWriteDeadline(time.Time{})
			if err != nil {
				log.Printf("TCP write error %s: %v", cl.callsign, err)
			}
		}(c, line)
	}
}

func keepaliveLoop() {
	for {
		time.Sleep(20 * time.Second)
		config.RLock()
		msg := fmt.Sprintf("# %s %s", config.ServerName, config.SoftwareVers)
		config.RUnlock()

		// WebSocket clients
		data, _ := json.Marshal(wsMessage{Type: "sys", Packet: msg})
		clientsMu.Lock()
		for c := range clients {
			select {
			case c.send <- data:
			default:
			}
		}
		clientsMu.Unlock()

		// TCP APRS-IS clients — raw keepalive comment
		tcpClientsMu.Lock()
		snap := make([]*tcpClient, 0, len(tcpClients))
		for c := range tcpClients {
			snap = append(snap, c)
		}
		tcpClientsMu.Unlock()
		for _, c := range snap {
			go func(cl *tcpClient, m string) {
				cl.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				fmt.Fprintf(cl.conn, "%s\r\n", m)
				cl.conn.SetWriteDeadline(time.Time{})
			}(c, msg)
		}
	}
}

func handleHistory(w http.ResponseWriter, r *http.Request) {
	historyMu.RLock()
	defer historyMu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(history)
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	metrics.RLock()
	uptime := time.Since(metrics.StartTime).Round(time.Second).String()
	res := map[string]interface{}{
		"uptime":   uptime,
		"pkts_rx":  metrics.PktsRx,
		"pkts_tx":  metrics.PktsTx,
		"dropped":  metrics.PktsDropped,
		"bytes_rx": metrics.BytesRx,
		"bytes_tx": metrics.BytesTx,
	}
	metrics.RUnlock()
	config.RLock()
	res["upstream_addr"] = config.UpstreamAddr
	// Public status must expose capability state, never integration credentials.
	res["ais_configured"] = (config.AISStreamKey != "")
	res["qrz_configured"] = (config.QRZUsername != "" && config.QRZPassword != "")
	res["metoffice_configured"] = (config.MetOfficeKey != "")
	res["center_lat"] = config.CenterLat
	res["center_lon"] = config.CenterLon
	res["drop_pistar"] = config.DropPiStar
	res["drop_dstar"] = config.DropDStar
	res["drop_apdesk"] = config.DropAPDesk
	config.RUnlock()
	res["upstream_connected"] = atomic.LoadInt32(&upstreamConnected) == 1
	rawRingMu.RLock()
	res["ring_size"] = len(rawRing)
	rawRingMu.RUnlock()
	active := []map[string]interface{}{}
	clientsMu.Lock()
	for c := range clients {
		if c.authenticated {
			active = append(active, map[string]interface{}{
				"call": c.callsign, "soft": c.software, "type": "websocket",
				"addr": c.remoteAddr, "since": c.connectedAt,
			})
		}
	}
	clientsMu.Unlock()
	tcpClientsMu.Lock()
	for c := range tcpClients {
		if c.callsign != "" {
			active = append(active, map[string]interface{}{
				"call": c.callsign, "soft": "TCP/APRS-IS", "type": "tcp",
				"addr": c.remoteAddr, "since": c.connectedAt,
				"verified": c.verified,
			})
		}
	}
	res["tcp_clients"] = len(tcpClients)
	tcpClientsMu.Unlock()
	res["clients"] = active
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "GET" {
		config.RLock()
		defer config.RUnlock()
		json.NewEncoder(w).Encode(&config)
		return
	}
	var n AppConfig
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	config.Lock()
	config.ServerName = n.ServerName
	config.SoftwareVers = n.SoftwareVers
	config.Callsign = n.Callsign
	config.Passcode = n.Passcode
	config.UpstreamAddr = n.UpstreamAddr
	config.ServerFilter = strings.TrimSpace(n.ServerFilter)
	config.DropPiStar = n.DropPiStar
	config.DropDStar = n.DropDStar
	config.DropAPDesk = n.DropAPDesk
	config.EnableGeofence = n.EnableGeofence
	config.CenterLat = n.CenterLat
	config.CenterLon = n.CenterLon
	config.RadiusKm = n.RadiusKm
	if n.AISStreamKey != "" {
		config.AISStreamKey = strings.TrimSpace(n.AISStreamKey)
	}
	if n.QRZUsername != "" {
		config.QRZUsername = strings.TrimSpace(n.QRZUsername)
	}
	if n.QRZPassword != "" {
		config.QRZPassword = n.QRZPassword
	}
	if n.MetOfficeKey != "" {
		config.MetOfficeKey = strings.TrimSpace(n.MetOfficeKey)
	}
	config.Unlock()
	if err := saveConfig(); err != nil {
		log.Printf("Warning: config applied but failed to persist: %v", err)
	}
	select {
	case <-reconnectChan:
	default:
	}
	reconnectChan <- struct{}{}
	w.WriteHeader(http.StatusOK)
}

// handleVersion returns the running server version.
// The frontend polls GitHub releases to compare and show an update badge.
func handleVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"version":"` + AppVersion + `"}`))
}

// handleARISS proxies the aprs.fi API for ARISS packets to avoid browser CORS issues.
func handleARISS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	url := "https://api.aprs.fi/api/get?name=RS0ISS,RS0ISS-4,NA1SS&what=loc&apikey=104710.mxTOVTFLMl6la5&format=json&howmany=50"
	resp, err := fetchURL(url)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadGateway)
		return
	}
	w.Write(resp)
}

// handleISSPosition proxies wheretheiss.at for live ISS position.
func handleISSPosition(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	resp, err := fetchURL("https://api.wheretheiss.at/v1/satellites/25544")
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadGateway)
		return
	}
	w.Write(resp)
}

// fetchURL makes a simple GET request and returns the body bytes.
func fetchURL(url string) ([]byte, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	buf := make([]byte, 0, 65536)
	buf2 := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf2)
		if n > 0 {
			buf = append(buf, buf2[:n]...)
		}
		if err != nil {
			break
		}
	}
	return buf, nil
}

// handleMessages returns the last hour of APRS messages.
func handleMessages(w http.ResponseWriter, r *http.Request) {
	msgStoreMu.RLock()
	defer msgStoreMu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgStore)
}

// handleARISS proxies ARISS packet data from aprs.fi API server-side.
// This avoids CORS issues with direct browser requests to aprs.fi.

// handleConfigDemo returns server config with sensitive fields redacted.
// No authentication required — safe to expose publicly for demo mode.
func handleConfigDemo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	config.RLock()
	redacted := map[string]interface{}{
		"server_name":     config.ServerName,
		"software_vers":   config.SoftwareVers,
		"callsign":        config.Callsign,
		"passcode":        "••••••",
		"upstream_addr":   config.UpstreamAddr,
		"server_filter":   config.ServerFilter,
		"drop_pistar":     config.DropPiStar,
		"drop_dstar":      config.DropDStar,
		"drop_apdesk":     config.DropAPDesk,
		"enable_geofence": config.EnableGeofence,
		"center_lat":      config.CenterLat,
		"center_lon":      config.CenterLon,
		"radius_km":       config.RadiusKm,
	}
	config.RUnlock()
	json.NewEncoder(w).Encode(redacted)
}

// handleTLE proxies TLE data from Celestrak for ISS and ARISS satellites.
// Caches for 6 hours since TLEs change slowly.
var (
	tleCache   string
	tleCacheAt time.Time
	tleCacheMu sync.Mutex
)

func handleTLE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "max-age=3600")

	tleCacheMu.Lock()
	defer tleCacheMu.Unlock()

	// Return cache if fresh (< 6 hours)
	if tleCache != "" && time.Since(tleCacheAt) < 6*time.Hour {
		w.Write([]byte(tleCache))
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}

	// Primary: live.ariss.org - always has current ISS TLE in 3-line format
	if resp, err := client.Get("https://live.ariss.org/iss.txt"); err == nil {
		if body, err := io.ReadAll(resp.Body); err == nil && len(body) > 50 {
			resp.Body.Close()
			tleCache = string(body)
			tleCacheAt = time.Now()
			w.Write(body)
			return
		}
		resp.Body.Close()
	}

	// Secondary: wheretheiss.at JSON API - extract TLE fields
	if resp, err := client.Get("https://api.wheretheiss.at/v1/satellites/25544/tles?units=miles"); err == nil {
		var result map[string]interface{}
		if err2 := json.NewDecoder(resp.Body).Decode(&result); err2 == nil {
			resp.Body.Close()
			name, _ := result["name"].(string)
			l1, _ := result["line1"].(string)
			l2, _ := result["line2"].(string)
			if l1 != "" && l2 != "" {
				text := name + "\n" + l1 + "\n" + l2 + "\n"
				tleCache = text
				tleCacheAt = time.Now()
				w.Write([]byte(text))
				return
			}
		} else {
			resp.Body.Close()
		}
	}

	// Tertiary: Celestrak (may be slow/blocked from some servers)
	for _, url := range []string{
		"https://celestrak.org/SATCAT/tle.php?CATNR=25544&FORMAT=TLE",
		"https://celestrak.com/SATCAT/tle.php?CATNR=25544&FORMAT=TLE",
	} {
		if resp, err := client.Get(url); err == nil {
			if body, err := io.ReadAll(resp.Body); err == nil && len(body) > 50 {
				resp.Body.Close()
				tleCache = string(body)
				tleCacheAt = time.Now()
				w.Write(body)
				return
			}
			resp.Body.Close()
		}
	}

	http.Error(w, "Could not fetch TLE from any source", 502)
}

// handleWhoami returns the authenticated username — used by the frontend
// to verify credentials are correct before showing the admin panel.
func handleWhoami(w http.ResponseWriter, r *http.Request) {
	adminCreds.RLock()
	user := adminCreds.Username
	adminCreds.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true,"user":"` + user + `"}`))
}

// handleUpdate performs a live update: git pull, go build, systemctl restart.
// Streams log lines as newline-delimited JSON so the browser can show progress.
// Protected by basicAuth.
func handleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, canFlush := w.(http.Flusher)

	send := func(msg, status string) {
		fmt.Fprintf(w, "data: %s\n\n", mustJSON(map[string]string{"msg": msg, "status": status}))
		if canFlush {
			flusher.Flush()
		}
		log.Printf("[update] %s", msg)
	}

	appDir := "/opt/aprs-gateway"
	gobin := "/usr/local/go/bin/go"

	send("Starting update...", "running")

	// Step 1: git pull
	send("Running git pull...", "running")
	out, err := runCmd(appDir, "git", "pull", "origin", "main")
	if err != nil {
		send("git pull failed: "+out, "error")
		return
	}
	send("git pull: "+strings.TrimSpace(out), "running")

	// Step 2: go build
	send("Building binary (this takes ~30s)...", "running")
	out, err = runCmd(appDir, gobin, "build", "-o", "aprs_server", "aprs_server.go")
	if err != nil {
		send("Build failed: "+out, "error")
		return
	}
	send("Build successful", "running")

	// Step 3: systemctl restart (runs async — connection will drop)
	send("Restarting service... reconnect in 5 seconds", "restarting")
	if canFlush {
		flusher.Flush()
	}
	go func() {
		time.Sleep(500 * time.Millisecond)
		exec.Command("systemctl", "restart", "aprs").Run()
	}()
}

func runCmd(dir string, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	return buf.String(), err
}

func mustJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// ─── Server config persistence ───────────────────────────────────────────────

// saveConfig writes the current AppConfig to server_config.json.
func saveConfig() error {
	config.RLock()
	data, err := json.MarshalIndent(&config, "", "  ")
	config.RUnlock()
	if err != nil {
		return err
	}
	return os.WriteFile(configFile, data, 0600)
}

// loadSavedConfig reads server_config.json if it exists and applies it.
// Called once at startup before the upstream connection is established.
func loadSavedConfig() {
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Printf("No server_config.json found — using defaults")
		return
	}
	var saved AppConfig
	if err := json.Unmarshal(data, &saved); err != nil {
		log.Printf("server_config.json parse error: %v — using defaults", err)
		return
	}
	config.Lock()
	config.ServerName = saved.ServerName
	config.SoftwareVers = saved.SoftwareVers
	config.Callsign = saved.Callsign
	config.Passcode = saved.Passcode
	config.UpstreamAddr = saved.UpstreamAddr
	config.ServerFilter = strings.TrimSpace(saved.ServerFilter)
	config.DropPiStar = saved.DropPiStar
	config.DropDStar = saved.DropDStar
	config.DropAPDesk = saved.DropAPDesk
	config.EnableGeofence = saved.EnableGeofence
	config.CenterLat = saved.CenterLat
	config.CenterLon = saved.CenterLon
	config.RadiusKm = saved.RadiusKm
	if saved.AISStreamKey != "" {
		config.AISStreamKey = saved.AISStreamKey
	}
	if saved.MetOfficeKey != "" {
		config.MetOfficeKey = saved.MetOfficeKey
	}
	if saved.QRZUsername != "" {
		config.QRZUsername = saved.QRZUsername
	}
	if saved.QRZPassword != "" {
		config.QRZPassword = saved.QRZPassword
	}
	config.Unlock()
	log.Printf("Server config loaded: callsign=%s upstream=%s filter=%s",
		saved.Callsign, saved.UpstreamAddr, saved.ServerFilter)
}

// ─── First-run credential persistence ────────────────────────────────────────

const credsFile = "creds.json"
const configFile = "server_config.json"

type storedCreds struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// credsConfigured returns true if creds.json exists and has non-empty values.
func credsConfigured() bool {
	adminCreds.RLock()
	defer adminCreds.RUnlock()
	return adminCreds.Username != "" && adminCreds.Password != ""
}

// loadOrInitCreds loads creds.json if it exists; otherwise sets empty creds
// so the server redirects to /setup on first visit.
func loadOrInitCreds() {
	data, err := os.ReadFile(credsFile)
	if err != nil {
		// First run — no creds file yet
		log.Printf("No creds.json found — first-run setup required at /setup")
		adminCreds.Lock()
		adminCreds.Username = ""
		adminCreds.Password = ""
		adminCreds.Unlock()
		return
	}
	var sc storedCreds
	if err := json.Unmarshal(data, &sc); err != nil || sc.Username == "" || sc.Password == "" {
		log.Printf("creds.json invalid — first-run setup required at /setup")
		adminCreds.Lock()
		adminCreds.Username = ""
		adminCreds.Password = ""
		adminCreds.Unlock()
		return
	}
	adminCreds.Lock()
	adminCreds.Username = sc.Username
	adminCreds.Password = sc.Password
	adminCreds.Unlock()
	log.Printf("Admin credentials loaded: %s", sc.Username)
}

// saveCreds writes current credentials to creds.json.
func saveCreds(username, password string) error {
	data, err := json.MarshalIndent(storedCreds{Username: username, Password: password}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(credsFile, data, 0600)
}

// handleSetup serves the first-run setup page and processes the form POST.
// Once creds are set it redirects to / permanently.
func handleSetup(w http.ResponseWriter, r *http.Request) {
	// If already configured, redirect away — setup is one-time only.
	if credsConfigured() {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	if r.Method == "POST" {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		username := strings.TrimSpace(r.FormValue("username"))
		password := r.FormValue("password")
		confirm := r.FormValue("confirm")

		var errMsg string
		switch {
		case username == "":
			errMsg = "Username cannot be empty."
		case len(username) < 3:
			errMsg = "Username must be at least 3 characters."
		case len(password) < 8:
			errMsg = "Password must be at least 8 characters."
		case password != confirm:
			errMsg = "Passwords do not match."
		}

		if errMsg != "" {
			serveSetupPage(w, errMsg)
			return
		}

		if err := saveCreds(username, password); err != nil {
			log.Printf("Failed to save creds: %v", err)
			http.Error(w, "Failed to save credentials", http.StatusInternalServerError)
			return
		}
		adminCreds.Lock()
		adminCreds.Username = username
		adminCreds.Password = password
		adminCreds.Unlock()
		log.Printf("First-run setup complete — admin user: %s", username)
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	serveSetupPage(w, "")
}

// handlePassword now also persists the new password to creds.json.
func handlePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		NewPassword string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.NewPassword) == "" {
		http.Error(w, "bad request: new_password required", http.StatusBadRequest)
		return
	}
	adminCreds.Lock()
	username := adminCreds.Username
	adminCreds.Password = req.NewPassword
	adminCreds.Unlock()
	if err := saveCreds(username, req.NewPassword); err != nil {
		log.Printf("Warning: password changed in memory but failed to persist: %v", err)
	}
	log.Printf("Admin password updated and persisted")
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

func serveSetupPage(w http.ResponseWriter, errMsg string) {
	errHTML := ""
	if errMsg != "" {
		errHTML = `<p class="text-red-400 text-sm mb-4 bg-red-900/30 p-3 rounded border border-red-700">` + errMsg + `</p>`
	}
	page := `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>APRS Gateway — First Run Setup</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#111827;color:#f9fafb;font-family:system-ui,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:1rem}
.card{background:#1f2937;border:1px solid #374151;border-radius:12px;padding:2.5rem;width:100%;max-width:420px;box-shadow:0 25px 50px rgba(0,0,0,.5)}
.logo{text-align:center;margin-bottom:2rem}
.logo .icon{font-size:3rem;margin-bottom:.5rem}
.logo h1{font-size:1.5rem;font-weight:700;color:#60a5fa}
.logo p{color:#9ca3af;font-size:.875rem;margin-top:.25rem}
label{display:block;font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em;color:#9ca3af;margin-bottom:.4rem;margin-top:1.25rem}
input{width:100%;background:#374151;border:1px solid #4b5563;border-radius:6px;padding:.75rem;color:#fff;font-size:.875rem;outline:none;transition:border-color .2s}
input:focus{border-color:#3b82f6}
.err{background:rgba(127,29,29,.4);border:1px solid #991b1b;color:#fca5a5;padding:.75rem 1rem;border-radius:6px;font-size:.875rem;margin-top:1rem}
button{width:100%;margin-top:1.5rem;background:#2563eb;color:#fff;border:none;padding:.875rem;border-radius:6px;font-weight:700;font-size:.875rem;text-transform:uppercase;letter-spacing:.1em;cursor:pointer;transition:background .2s}
button:hover{background:#1d4ed8}
.note{text-align:center;font-size:.7rem;color:#6b7280;margin-top:1.5rem}
.note code{color:#9ca3af}
</style>
</head>
<body>
<div class="card">
  <div class="logo">
    <div class="icon">📡</div>
    <h1>APRS Gateway</h1>
    <p>First-run setup — create your admin account</p>
  </div>
  ` + errHTML + `
  <form method="POST" action="/setup">
    <label>Admin Username</label>
    <input type="text" name="username" required minlength="3" autocomplete="username" placeholder="e.g. 2e0lxy">
    <label>Password</label>
    <input type="password" name="password" required minlength="8" autocomplete="new-password" placeholder="Minimum 8 characters">
    <label>Confirm Password</label>
    <input type="password" name="confirm" required autocomplete="new-password" placeholder="Repeat password">
    <button type="submit">Create Account &amp; Launch Gateway</button>
  </form>
  <p class="note">Credentials stored in <code>creds.json</code> on the server. Shown once only.</p>
</div>
</body>
</html>`
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(page))
}

// ─── Prometheus Metrics ───────────────────────────────────────────────────────

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	clientsMu.Lock()
	wsClients := len(clients)
	clientsMu.Unlock()

	historyMu.RLock()
	histLen := len(history)
	historyMu.RUnlock()

	rawRingMu.RLock()
	ringLen := len(rawRing)
	rawRingMu.RUnlock()

	objectStoreMu.RLock()
	objCount := len(objectStore)
	objectStoreMu.RUnlock()

	upSecs := time.Since(metricsStart).Seconds()
	pkts := atomic.LoadInt64(&totalPackets)
	dupes := atomic.LoadInt64(&dupePackets)
	bytes := atomic.LoadInt64(&upstreamBytes)
	upConn := atomic.LoadInt32(&upstreamConnected)

	config.RLock()
	call := config.Callsign
	filter := config.ServerFilter
	config.RUnlock()

	fmt.Fprintf(w, "# HELP aprs_packets_total Total APRS packets received from upstream\n")
	fmt.Fprintf(w, "# TYPE aprs_packets_total counter\n")
	fmt.Fprintf(w, "aprs_packets_total{callsign=%q} %d\n\n", call, pkts)

	fmt.Fprintf(w, "# HELP aprs_dupe_packets_total Duplicate packets dropped\n")
	fmt.Fprintf(w, "# TYPE aprs_dupe_packets_total counter\n")
	fmt.Fprintf(w, "aprs_dupe_packets_total{callsign=%q} %d\n\n", call, dupes)

	fmt.Fprintf(w, "# HELP aprs_upstream_bytes_total Bytes received from APRS-IS upstream\n")
	fmt.Fprintf(w, "# TYPE aprs_upstream_bytes_total counter\n")
	fmt.Fprintf(w, "aprs_upstream_bytes_total{callsign=%q} %d\n\n", call, bytes)

	fmt.Fprintf(w, "# HELP aprs_upstream_connected 1 if connected to APRS-IS upstream, 0 otherwise\n")
	fmt.Fprintf(w, "# TYPE aprs_upstream_connected gauge\n")
	fmt.Fprintf(w, "aprs_upstream_connected{callsign=%q,filter=%q} %d\n\n", call, filter, upConn)

	fmt.Fprintf(w, "# HELP aprs_websocket_clients Current WebSocket client connections\n")
	fmt.Fprintf(w, "# TYPE aprs_websocket_clients gauge\n")
	fmt.Fprintf(w, "aprs_websocket_clients %d\n\n", wsClients)

	fmt.Fprintf(w, "# HELP aprs_history_packets Packets in position history store\n")
	fmt.Fprintf(w, "# TYPE aprs_history_packets gauge\n")
	fmt.Fprintf(w, "aprs_history_packets %d\n\n", histLen)

	fmt.Fprintf(w, "# HELP aprs_raw_ring_packets Packets in 1-hour raw ring buffer\n")
	fmt.Fprintf(w, "# TYPE aprs_raw_ring_packets gauge\n")
	fmt.Fprintf(w, "aprs_raw_ring_packets %d\n\n", ringLen)

	fmt.Fprintf(w, "# HELP aprs_objects_active Active APRS objects/items on map\n")
	fmt.Fprintf(w, "# TYPE aprs_objects_active gauge\n")
	fmt.Fprintf(w, "aprs_objects_active %d\n\n", objCount)

	fmt.Fprintf(w, "# HELP aprs_uptime_seconds Seconds since server start\n")
	fmt.Fprintf(w, "# TYPE aprs_uptime_seconds counter\n")
	fmt.Fprintf(w, "aprs_uptime_seconds %.0f\n\n", upSecs)

	pktRate := 0.0
	if upSecs > 0 {
		pktRate = float64(pkts) / upSecs
	}
	fmt.Fprintf(w, "# HELP aprs_packets_per_second Average packets per second since start\n")
	fmt.Fprintf(w, "# TYPE aprs_packets_per_second gauge\n")
	fmt.Fprintf(w, "aprs_packets_per_second %.3f\n\n", pktRate)

	fmt.Fprintf(w, "# HELP aprs_mqtt_connections Current open MQTT connections\n")
	fmt.Fprintf(w, "# TYPE aprs_mqtt_connections gauge\n")
	fmt.Fprintf(w, "aprs_mqtt_connections %d\n\n", atomic.LoadInt64(&mqttActiveConnections))

	fmt.Fprintf(w, "# HELP aprs_mqtt_auth_failures_total MQTT logins rejected for invalid credentials\n")
	fmt.Fprintf(w, "# TYPE aprs_mqtt_auth_failures_total counter\n")
	fmt.Fprintf(w, "aprs_mqtt_auth_failures_total %d\n\n", atomic.LoadInt64(&mqttAuthFailures))

	fmt.Fprintf(w, "# HELP aprs_mqtt_connections_rejected_total MQTT connections rejected by safety limits\n")
	fmt.Fprintf(w, "# TYPE aprs_mqtt_connections_rejected_total counter\n")
	fmt.Fprintf(w, "aprs_mqtt_connections_rejected_total %d\n\n", atomic.LoadInt64(&mqttConnectionsRejected))

	fmt.Fprintf(w, "# HELP aprs_internal_queue_utilization_ratio Highest packet queue utilization ratio\n")
	fmt.Fprintf(w, "# TYPE aprs_internal_queue_utilization_ratio gauge\n")
	fmt.Fprintf(w, "aprs_internal_queue_utilization_ratio %.4f\n\n", maxQueuePercent(PerformanceSample{
		BroadcastQueue: len(broadcast), BroadcastQueueCap: cap(broadcast),
		UpstreamQueue: len(upstreamOut), UpstreamQueueCap: cap(upstreamOut),
		UpstreamTxQueue: len(upstreamTx), UpstreamTxQueueCap: cap(upstreamTx),
	})/100)
}

// ─── GeoJSON Export ───────────────────────────────────────────────────────────

func handleGeoJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/geo+json")
	w.Header().Set("Content-Disposition", `attachment; filename="aprs-stations.geojson"`)

	historyMu.RLock()
	snap := make([]HistoryPacket, len(history))
	copy(snap, history)
	historyMu.RUnlock()

	// Deduplicate - latest position per callsign
	latest := make(map[string]HistoryPacket)
	for _, p := range snap {
		if _, ok := latest[p.Callsign]; !ok {
			latest[p.Callsign] = p
		}
		if p.Timestamp > latest[p.Callsign].Timestamp {
			latest[p.Callsign] = p
		}
	}

	type GeoFeature struct {
		Type       string                 `json:"type"`
		Geometry   map[string]interface{} `json:"geometry"`
		Properties map[string]interface{} `json:"properties"`
	}
	type GeoCollection struct {
		Type     string       `json:"type"`
		Features []GeoFeature `json:"features"`
	}

	fc := GeoCollection{Type: "FeatureCollection"}
	for _, p := range latest {
		fc.Features = append(fc.Features, GeoFeature{
			Type: "Feature",
			Geometry: map[string]interface{}{
				"type":        "Point",
				"coordinates": []float64{p.Lon, p.Lat},
			},
			Properties: map[string]interface{}{
				"callsign":  p.Callsign,
				"symbol":    p.Symbol,
				"path":      p.Path,
				"timestamp": p.Timestamp,
				"raw":       p.Raw,
			},
		})
	}

	// Add objects
	objectStoreMu.RLock()
	for _, o := range objectStore {
		if !o.Killed {
			fc.Features = append(fc.Features, GeoFeature{
				Type: "Feature",
				Geometry: map[string]interface{}{
					"type":        "Point",
					"coordinates": []float64{o.Lon, o.Lat},
				},
				Properties: map[string]interface{}{
					"callsign":  o.Name,
					"name":      o.Name,
					"owner":     o.Owner,
					"symbol":    o.Symbol,
					"comment":   o.Comment,
					"timestamp": o.Timestamp,
					"type":      "object",
				},
			})
		}
	}
	objectStoreMu.RUnlock()

	json.NewEncoder(w).Encode(fc)
}

// ─── KML Export ───────────────────────────────────────────────────────────────

func handleKML(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/vnd.google-earth.kml+xml")
	w.Header().Set("Content-Disposition", `attachment; filename="aprs-stations.kml"`)

	historyMu.RLock()
	snap := make([]HistoryPacket, len(history))
	copy(snap, history)
	historyMu.RUnlock()

	latest := make(map[string]HistoryPacket)
	for _, p := range snap {
		if ex, ok := latest[p.Callsign]; !ok || p.Timestamp > ex.Timestamp {
			latest[p.Callsign] = p
		}
	}

	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
<name>APRS Stations</name>
<description>Exported from Advanced APRS Go Server</description>
`)
	for _, p := range latest {
		fmt.Fprintf(w, `<Placemark>
  <name>%s</name>
  <description>%s&#10;Path: %s</description>
  <Point><coordinates>%f,%f,0</coordinates></Point>
</Placemark>
`, p.Callsign, p.Raw, p.Path, p.Lon, p.Lat)
	}

	objectStoreMu.RLock()
	for _, o := range objectStore {
		if !o.Killed {
			fmt.Fprintf(w, `<Placemark>
  <name>%s (Object)</name>
  <description>Owner: %s&#10;%s</description>
  <Point><coordinates>%f,%f,0</coordinates></Point>
</Placemark>
`, o.Name, o.Owner, o.Comment, o.Lon, o.Lat)
		}
	}
	objectStoreMu.RUnlock()

	fmt.Fprintf(w, "</Document>\n</kml>\n")
}

// ─── Webhooks & API Keys ──────────────────────────────────────────────────────

const webhooksFile = "webhooks.json"
const apiKeysFile = "apikeys.json"

func loadWebhooksAndKeys() {
	// Load webhooks
	if data, err := os.ReadFile(webhooksFile); err == nil {
		webhooksMu.Lock()
		json.Unmarshal(data, &webhooks)
		webhooksMu.Unlock()
		log.Printf("Loaded %d webhooks", len(webhooks))
	}
	// Load API keys
	if data, err := os.ReadFile(apiKeysFile); err == nil {
		apiKeysMu.Lock()
		json.Unmarshal(data, &apiKeys)
		apiKeysMu.Unlock()
		log.Printf("Loaded %d API keys", len(apiKeys))
	}
}

func saveWebhooks() {
	webhooksMu.RLock()
	data, _ := json.MarshalIndent(webhooks, "", "  ")
	webhooksMu.RUnlock()
	os.WriteFile(webhooksFile, data, 0600)
}

func saveAPIKeys() {
	apiKeysMu.RLock()
	data, _ := json.MarshalIndent(apiKeys, "", "  ")
	apiKeysMu.RUnlock()
	os.WriteFile(apiKeysFile, data, 0600)
}

func generateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func generateAPIKey() string {
	b := make([]byte, 24)
	rand.Read(b)
	return "aprs_" + fmt.Sprintf("%x", b)
}

// handleWebhooks - GET returns all webhooks, POST creates one
func handleWebhooks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		webhooksMu.RLock()
		json.NewEncoder(w).Encode(webhooks)
		webhooksMu.RUnlock()

	case http.MethodPost:
		var wh WebhookConfig
		if err := json.NewDecoder(r.Body).Decode(&wh); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		if wh.URL == "" {
			http.Error(w, `{"error":"url required"}`, 400)
			return
		}
		wh.ID = generateID()
		wh.Enabled = true
		webhooksMu.Lock()
		webhooks = append(webhooks, wh)
		webhooksMu.Unlock()
		saveWebhooks()
		json.NewEncoder(w).Encode(wh)

	default:
		http.Error(w, "method not allowed", 405)
	}
}

// handleWebhookDelete - DELETE /api/webhooks/{id}
func handleWebhookDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", 405)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/webhooks/")
	webhooksMu.Lock()
	newWH := webhooks[:0]
	for _, wh := range webhooks {
		if wh.ID != id {
			newWH = append(newWH, wh)
		}
	}
	webhooks = newWH
	webhooksMu.Unlock()
	saveWebhooks()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

// handleAPIKeys - GET returns all keys (with key masked), POST creates one
func handleAPIKeys(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		apiKeysMu.RLock()
		result := make([]map[string]interface{}, 0)
		for _, k := range apiKeys {
			// Mask key - only show prefix
			masked := k.Key[:12] + "..."
			result = append(result, map[string]interface{}{
				"id": k.ID, "name": k.Name, "key": masked,
				"created": k.Created, "last_used": k.LastUsed, "read_only": k.ReadOnly,
			})
		}
		apiKeysMu.RUnlock()
		json.NewEncoder(w).Encode(result)

	case http.MethodPost:
		var req struct {
			Name     string `json:"name"`
			ReadOnly bool   `json:"read_only"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, 400)
			return
		}
		if req.Name == "" {
			req.Name = "API Key"
		}
		k := APIKey{
			ID: generateID(), Name: req.Name,
			Key: generateAPIKey(), Created: time.Now().Unix(),
			ReadOnly: req.ReadOnly,
		}
		apiKeysMu.Lock()
		apiKeys[k.Key] = k
		apiKeysMu.Unlock()
		saveAPIKeys()
		// Return full key ONCE on creation
		json.NewEncoder(w).Encode(k)

	default:
		http.Error(w, "method not allowed", 405)
	}
}

// handleAPIKeyDelete - DELETE /api/keys/{id}
func handleAPIKeyDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", 405)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/keys/")
	apiKeysMu.Lock()
	for key, k := range apiKeys {
		if k.ID == id {
			delete(apiKeys, key)
			break
		}
	}
	apiKeysMu.Unlock()
	saveAPIKeys()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

// apiKeyAuth middleware - accepts either Basic Auth OR X-API-Key header
func apiKeyAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Try X-API-Key header
		key := r.Header.Get("X-API-Key")
		if key == "" {
			key = r.URL.Query().Get("api_key")
		}
		if key != "" {
			apiKeysMu.Lock()
			k, ok := apiKeys[key]
			if ok {
				k.LastUsed = time.Now().Unix()
				apiKeys[key] = k
				apiKeysMu.Unlock()
				next(w, r)
				return
			}
			apiKeysMu.Unlock()
		}
		// Fall through to basic auth
		user, pass, ok := r.BasicAuth()
		adminCreds.RLock()
		wantUser, wantPass := adminCreds.Username, adminCreds.Password
		adminCreds.RUnlock()
		if !ok || user != wantUser || pass != wantPass {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		next(w, r)
	}
}

// fireWebhooks sends an event to all matching enabled webhooks
func fireWebhooks(event string, callsign string, data interface{}) {
	webhooksMu.RLock()
	whs := make([]WebhookConfig, len(webhooks))
	copy(whs, webhooks)
	webhooksMu.RUnlock()

	payload := WebhookEvent{
		Event:     event,
		Timestamp: time.Now().Unix(),
		Callsign:  callsign,
		Data:      data,
	}
	body, _ := json.Marshal(payload)

	for i, wh := range whs {
		if !wh.Enabled {
			continue
		}
		// Check callsign filter (empty = all)
		if wh.Callsign != "" && !strings.EqualFold(wh.Callsign, callsign) &&
			!strings.HasSuffix(callsign, wh.Callsign) {
			continue
		}
		// Check event filter
		if len(wh.Events) > 0 {
			matched := false
			for _, e := range wh.Events {
				if e == event || e == "*" {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}

		go func(wh WebhookConfig, idx int) {
			client := &http.Client{Timeout: 8 * time.Second}
			req, err := http.NewRequest("POST", wh.URL, strings.NewReader(string(body)))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-APRS-Event", event)
			req.Header.Set("X-APRS-Callsign", callsign)
			if wh.Secret != "" {
				req.Header.Set("X-APRS-Secret", wh.Secret)
			}
			resp, err := client.Do(req)
			status := 0
			if err == nil {
				status = resp.StatusCode
				resp.Body.Close()
			}
			webhooksMu.Lock()
			for j := range webhooks {
				if webhooks[j].ID == wh.ID {
					webhooks[j].LastFired = time.Now().Unix()
					webhooks[j].LastStatus = status
					break
				}
			}
			webhooksMu.Unlock()
			saveWebhooks()
		}(wh, i)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// MEMBER SYSTEM
// ═══════════════════════════════════════════════════════════════════════════════

const membersFile = "members.json"
const sessionTTL = 30 * 24 * time.Hour // 30 day sessions

// ── Types ─────────────────────────────────────────────────────────────────────

type Member struct {
	ID        string   `json:"id"`
	Callsign  string   `json:"callsign"` // primary callsign (login)
	Password  string   `json:"password"` // bcrypt-style: sha256+salt stored as hex
	Salt      string   `json:"salt"`
	Name      string   `json:"name,omitempty"`
	Email     string   `json:"email,omitempty"`
	Callsigns []string `json:"callsigns"` // all owned callsigns
	Passcode  int      `json:"passcode"`  // APRS-IS passcode
	Watchlist []string `json:"watchlist"`
	Created   int64    `json:"created"`
	LastLogin int64    `json:"last_login,omitempty"`
	Verified  bool     `json:"verified"`
	// Per-member map filter preferences. Free-form JSON blob; the web map
	// and Android client decide which keys they understand. Common keys:
	//   drop_pistar  - hide DV gateway beacons (Pi-Star, MMDVM, APDPRS,
	//                  DMRGateway, ircDDB)
	//   drop_dstar   - hide D-STAR repeater forwarding traffic
	//   drop_apdesk  - hide UI-View desktop client beacons
	//   show_aprs / show_cwop / show_ogn / show_objects / show_lora_aprsis /
	//   show_ships / show_lora / hide_static / cluster_enable / ghost_old
	// Defaults to nil for unmigrated members; clients then fall back to
	// their own defaults until the member saves once.
	Preferences map[string]interface{} `json:"preferences,omitempty"`
	AlertRules  []MemberAlertRule      `json:"alert_rules,omitempty"`
}

type StoredMessage struct {
	ID        string `json:"id"`
	From      string `json:"from"`
	To        string `json:"to"`
	Text      string `json:"text"`
	Ts        int64  `json:"ts"`
	Read      bool   `json:"read"`
	Raw       string `json:"raw,omitempty"`
	Direction string `json:"direction,omitempty"` // "in" (default/absent) | "out"
}

type MemberSession struct {
	Token    string `json:"token"`
	MemberID string `json:"member_id"`
	Expires  int64  `json:"expires"`
}

// RegisteredServer records a remote APRS Net gateway that has phoned home.
type RegisteredServer struct {
	IP           string `json:"ip"`
	Domain       string `json:"domain,omitempty"`
	Callsign     string `json:"callsign,omitempty"`
	ServerName   string `json:"server_name,omitempty"`
	Version      string `json:"version,omitempty"`
	StationCount int    `json:"station_count,omitempty"`
	RegisteredAt int64  `json:"registered_at"`
	LastSeenAt   int64  `json:"last_seen"`
}

type MemberStore struct {
	Members      map[string]*Member           `json:"members"`
	Sessions     map[string]*MemberSession    `json:"sessions"`
	Messages     map[string][]StoredMessage   `json:"messages"`
	KnownServers map[string]*RegisteredServer `json:"known_servers,omitempty"`
}

var (
	memberStore   *MemberStore
	memberStoreMu sync.RWMutex
)

// ── Persistence ───────────────────────────────────────────────────────────────

func loadMemberStore() {
	memberStoreMu.Lock()
	defer memberStoreMu.Unlock()
	memberStore = &MemberStore{
		Members:      make(map[string]*Member),
		Sessions:     make(map[string]*MemberSession),
		Messages:     make(map[string][]StoredMessage),
		KnownServers: make(map[string]*RegisteredServer),
	}
	data, err := os.ReadFile(membersFile)
	if err != nil {
		return
	}
	if err := json.Unmarshal(data, memberStore); err != nil {
		log.Printf("Warning: could not parse members.json: %v", err)
		memberStore = &MemberStore{
			Members:      make(map[string]*Member),
			Sessions:     make(map[string]*MemberSession),
			Messages:     make(map[string][]StoredMessage),
			KnownServers: make(map[string]*RegisteredServer),
		}
	}
	if memberStore.KnownServers == nil {
		memberStore.KnownServers = make(map[string]*RegisteredServer)
	}
	log.Printf("Loaded %d members", len(memberStore.Members))
}

func saveMemberStore() {
	data, err := json.MarshalIndent(memberStore, "", "  ")
	if err != nil {
		return
	}
	os.WriteFile(membersFile, data, 0600)
}

// ── Password hashing (no bcrypt dependency - use SHA256+salt) ─────────────────

// hashPassword uses SHA-256 with salt + 10000 iterations (PBKDF2-lite)
func hashPassword(password, salt string) string {
	data := []byte(password + ":" + salt)
	for i := 0; i < 10000; i++ {
		h := sha256.Sum256(data)
		data = h[:]
	}
	return hex.EncodeToString(data)
}

func calcAPRSPasscode(callsign string) int {
	call := strings.ToUpper(strings.SplitN(callsign, "-", 2)[0])
	hash := 0x73e2
	for i := 0; i < len(call); i += 2 {
		hash ^= int(call[i]) << 8
		if i+1 < len(call) {
			hash ^= int(call[i+1])
		}
	}
	return hash & 0x7fff
}

// ── Session helpers ───────────────────────────────────────────────────────────

func generateToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func getMemberFromRequest(r *http.Request) *Member {
	token := r.Header.Get("X-Member-Token")
	if token == "" {
		if c, err := r.Cookie("member_token"); err == nil {
			token = c.Value
		}
	}
	if token == "" {
		return nil
	}
	memberStoreMu.RLock()
	defer memberStoreMu.RUnlock()
	sess, ok := memberStore.Sessions[token]
	if !ok || sess.Expires < time.Now().Unix() {
		return nil
	}
	m, ok := memberStore.Members[sess.MemberID]
	if !ok {
		return nil
	}
	return m
}

func cleanExpiredSessions() {
	for {
		time.Sleep(1 * time.Hour)
		memberStoreMu.Lock()
		now := time.Now().Unix()
		for tok, sess := range memberStore.Sessions {
			if sess.Expires < now {
				delete(memberStore.Sessions, tok)
			}
		}
		saveMemberStore()
		memberStoreMu.Unlock()
	}
}

// ── Handlers ──────────────────────────────────────────────────────────────────

// POST /api/member/register
func handleMemberRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req struct {
		Callsign string `json:"callsign"`
		Password string `json:"password"`
		Name     string `json:"name"`
		Email    string `json:"email"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, 400)
		return
	}
	req.Callsign = strings.ToUpper(strings.TrimSpace(req.Callsign))
	if len(req.Callsign) < 3 || len(req.Password) < 6 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error": "callsign must be 3+ chars, password 6+ chars"})
		return
	}
	memberStoreMu.Lock()
	defer memberStoreMu.Unlock()
	// Check callsign not already registered
	for _, m := range memberStore.Members {
		if strings.ToUpper(m.Callsign) == req.Callsign {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(409)
			json.NewEncoder(w).Encode(map[string]string{"error": "callsign already registered"})
			return
		}
	}
	salt := generateToken()[:16]
	member := &Member{
		ID:        generateID(),
		Callsign:  req.Callsign,
		Password:  hashPassword(req.Password, salt),
		Salt:      salt,
		Name:      req.Name,
		Email:     req.Email,
		Callsigns: []string{req.Callsign},
		Passcode:  calcAPRSPasscode(req.Callsign),
		Watchlist: []string{},
		Created:   time.Now().Unix(),
		Verified:  true,
	}
	memberStore.Members[member.ID] = member
	saveMemberStore()
	go auditLog("system", "member.register", member.Callsign, "", getClientIP(r))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": true, "callsign": member.Callsign, "passcode": member.Passcode,
	})
}

// POST /api/member/login
func handleMemberLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req struct {
		Callsign string `json:"callsign"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, 400)
		return
	}
	req.Callsign = strings.ToUpper(strings.TrimSpace(req.Callsign))
	memberStoreMu.Lock()
	defer memberStoreMu.Unlock()
	var found *Member
	for _, m := range memberStore.Members {
		if strings.ToUpper(m.Callsign) == req.Callsign {
			found = m
			break
		}
	}
	if found == nil || hashPassword(req.Password, found.Salt) != found.Password {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(401)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid callsign or password"})
		return
	}
	token := generateToken()
	memberStore.Sessions[token] = &MemberSession{
		Token: token, MemberID: found.ID,
		Expires: time.Now().Add(sessionTTL).Unix(),
	}
	found.LastLogin = time.Now().Unix()
	saveMemberStore()
	http.SetCookie(w, &http.Cookie{
		Name: "member_token", Value: token,
		Expires: time.Now().Add(sessionTTL),
		Path:    "/", HttpOnly: true, SameSite: http.SameSiteLaxMode,
	})
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": true, "token": token,
		"callsign":  found.Callsign,
		"name":      found.Name,
		"passcode":  found.Passcode,
		"callsigns": found.Callsigns,
		"watchlist": found.Watchlist,
	})
}

// POST /api/member/logout
func handleMemberLogout(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("X-Member-Token")
	if token == "" {
		if c, err := r.Cookie("member_token"); err == nil {
			token = c.Value
		}
	}
	if token != "" {
		memberStoreMu.Lock()
		delete(memberStore.Sessions, token)
		saveMemberStore()
		memberStoreMu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: "member_token", Value: "", MaxAge: -1, Path: "/"})
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// GET /api/member/profile  PUT /api/member/profile
func handleMemberProfile(w http.ResponseWriter, r *http.Request) {
	m := getMemberFromRequest(r)
	if m == nil {
		w.WriteHeader(401)
		json.NewEncoder(w).Encode(map[string]string{"error": "not logged in"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		// Return profile including offline messages count
		memberStoreMu.RLock()
		msgs := memberStore.Messages[strings.ToUpper(m.Callsign)]
		unread := 0
		for _, msg := range msgs {
			if !msg.Read {
				unread++
			}
		}
		memberStoreMu.RUnlock()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id": m.ID, "callsign": m.Callsign, "name": m.Name, "email": m.Email,
			"callsigns": m.Callsigns, "passcode": m.Passcode,
			"watchlist": m.Watchlist, "created": m.Created, "last_login": m.LastLogin,
			"unread_messages": unread,
		})
		return
	}
	if r.Method == http.MethodPut {
		var req struct {
			Name      string   `json:"name"`
			Email     string   `json:"email"`
			Callsigns []string `json:"callsigns"`
			Watchlist []string `json:"watchlist"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"})
			return
		}
		memberStoreMu.Lock()
		if req.Name != "" {
			memberStore.Members[m.ID].Name = req.Name
		}
		if req.Email != "" {
			memberStore.Members[m.ID].Email = req.Email
		}
		if req.Callsigns != nil {
			memberStore.Members[m.ID].Callsigns = req.Callsigns
		}
		if req.Watchlist != nil {
			memberStore.Members[m.ID].Watchlist = req.Watchlist
		}
		saveMemberStore()
		memberStoreMu.Unlock()
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		return
	}
	http.Error(w, "method not allowed", 405)
}

// GET /api/member/messages  PATCH /api/member/messages (mark read)
func handleMemberMessages(w http.ResponseWriter, r *http.Request) {
	m := getMemberFromRequest(r)
	if m == nil {
		w.WriteHeader(401)
		json.NewEncoder(w).Encode(map[string]string{"error": "not logged in"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	call := strings.ToUpper(m.Callsign)
	if r.Method == http.MethodGet {
		memberStoreMu.RLock()
		msgs := memberStore.Messages[call]
		if msgs == nil {
			msgs = []StoredMessage{}
		}
		memberStoreMu.RUnlock()
		json.NewEncoder(w).Encode(msgs)
		return
	}
	if r.Method == http.MethodPatch {
		// Mark all as read
		memberStoreMu.Lock()
		for i := range memberStore.Messages[call] {
			memberStore.Messages[call][i].Read = true
		}
		saveMemberStore()
		memberStoreMu.Unlock()
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		return
	}
	if r.Method == http.MethodDelete {
		remote := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("callsign")))
		if remote == "" {
			http.Error(w, "callsign required", 400)
			return
		}
		memberStoreMu.Lock()
		var kept []StoredMessage
		for _, msg := range memberStore.Messages[call] {
			if strings.ToUpper(msg.From) != remote && strings.ToUpper(msg.To) != remote {
				kept = append(kept, msg)
			}
		}
		if len(kept) == 0 {
			delete(memberStore.Messages, call)
		} else {
			memberStore.Messages[call] = kept
		}
		var remoteKept []StoredMessage
		for _, msg := range memberStore.Messages[remote] {
			if strings.ToUpper(msg.From) != call && strings.ToUpper(msg.To) != call {
				remoteKept = append(remoteKept, msg)
			}
		}
		if len(remoteKept) == 0 {
			delete(memberStore.Messages, remote)
		} else {
			memberStore.Messages[remote] = remoteKept
		}
		saveMemberStore()
		memberStoreMu.Unlock()
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		return
	}
	http.Error(w, "method not allowed", 405)
}

// POST /api/member/password  (change password)
func handleMemberPassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	m := getMemberFromRequest(r)
	if m == nil {
		w.WriteHeader(401)
		json.NewEncoder(w).Encode(map[string]string{"error": "not logged in"})
		return
	}
	var req struct {
		Current string `json:"current"`
		New     string `json:"new"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"})
		return
	}
	memberStoreMu.Lock()
	defer memberStoreMu.Unlock()
	mem := memberStore.Members[m.ID]
	if hashPassword(req.Current, mem.Salt) != mem.Password {
		w.WriteHeader(401)
		json.NewEncoder(w).Encode(map[string]string{"error": "current password incorrect"})
		return
	}
	if len(req.New) < 6 {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error": "new password must be 6+ chars"})
		return
	}
	mem.Password = hashPassword(req.New, mem.Salt)
	saveMemberStore()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// storeMessageForMember stores an incoming APRS message for a registered member

// handleMemberObject lets a logged-in member transmit an APRS object
// (or item, or kill packet) to the APRS-IS network with their callsign.
func handleMemberObject(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}
	token := r.Header.Get("X-Member-Token")
	if token == "" {
		if ck, err := r.Cookie("aprs-member-token"); err == nil {
			token = ck.Value
		}
	}
	memberStoreMu.RLock()
	sess, ok := memberStore.Sessions[token]
	var mem *Member
	if ok {
		mem = memberStore.Members[sess.MemberID]
	}
	memberStoreMu.RUnlock()
	if !ok || sess.Expires < time.Now().Unix() || mem == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"not authenticated"}`))
		return
	}

	var req struct {
		Name    string  `json:"name"`
		Lat     float64 `json:"lat"`
		Lon     float64 `json:"lon"`
		Sym     string  `json:"sym"`
		Comment string  `json:"comment"`
		Killed  bool    `json:"killed"`
		Type    string  `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"bad json"}`, 400)
		return
	}

	req.Name = strings.ToUpper(strings.TrimSpace(req.Name))
	if len(req.Name) == 0 || len(req.Name) > 9 {
		http.Error(w, `{"error":"name 1-9 chars required"}`, 400)
		return
	}
	if req.Lat < -90 || req.Lat > 90 || req.Lon < -180 || req.Lon > 180 {
		http.Error(w, `{"error":"invalid coordinates"}`, 400)
		return
	}
	if len(req.Sym) != 2 {
		req.Sym = "/-"
	}
	if len(req.Comment) > 36 {
		req.Comment = req.Comment[:36]
	}

	name := req.Name
	for len(name) < 9 {
		name = name + " "
	}

	latStr, lonStr := formatAPRSPosition(req.Lat, req.Lon)

	indicator := "*"
	if req.Killed {
		indicator = "_"
	}

	ts := time.Now().UTC().Format("021504") + "z"

	payload := fmt.Sprintf(";%s%s%s%s%s%s%s",
		name, indicator, ts, latStr, string(req.Sym[0]), lonStr, string(req.Sym[1]))
	if req.Comment != "" {
		payload += req.Comment
	}

	srcCall := strings.ToUpper(mem.Callsign)
	packet := fmt.Sprintf("%s>APAGOR,TCPIP*:%s", srcCall, payload)

	// Inject into broadcast (so local clients see it immediately)
	select {
	case broadcast <- packet:
	default:
	}

	// Send upstream to APRS-IS via the upstreamTx channel (consumed by connectUpstream)
	select {
	case upstreamTx <- packet:
	default:
	}

	auditLog(mem.Callsign, "member_object", req.Name,
		fmt.Sprintf("lat=%.4f lon=%.4f sym=%s killed=%v", req.Lat, req.Lon, req.Sym, req.Killed),
		r.RemoteAddr)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":     true,
		"packet": packet,
	})
}

// formatAPRSPosition converts decimal lat/lon to APRS uncompressed text format.

// GET /api/member/preferences   PUT /api/member/preferences
// Reads or replaces the per-member map filter preferences blob.
// Auth: X-Member-Token header (existing pattern used by other member endpoints).
// On PUT the request body is a JSON object that completely REPLACES the
// previous preferences map. Keys are open-ended; clients decide which keys
// they recognise. See the Member struct doc for the recommended schema.
func handleMemberPreferences(w http.ResponseWriter, r *http.Request) {
	m := getMemberFromRequest(r)
	if m == nil {
		w.WriteHeader(401)
		json.NewEncoder(w).Encode(map[string]string{"error": "not logged in"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		memberStoreMu.RLock()
		prefs := memberStore.Members[m.ID].Preferences
		memberStoreMu.RUnlock()
		if prefs == nil {
			prefs = map[string]interface{}{}
		}
		json.NewEncoder(w).Encode(prefs)
		return
	}
	if r.Method == http.MethodPut {
		var incoming map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"})
			return
		}
		if incoming == nil {
			incoming = map[string]interface{}{}
		}
		memberStoreMu.Lock()
		if mem, ok := memberStore.Members[m.ID]; ok {
			// Merge incoming keys over existing prefs rather than replacing wholesale.
			// This prevents a partial update (e.g. Android pushing only UI prefs)
			// from wiping keys owned by other subsystems (e.g. WX station settings).
			if mem.Preferences == nil {
				mem.Preferences = make(map[string]interface{})
			}
			for k, v := range incoming {
				mem.Preferences[k] = v
			}
		}
		// Snapshot the merged prefs for the real-time push below.
		var prefs map[string]interface{}
		if mem, ok := memberStore.Members[m.ID]; ok {
			prefs = mem.Preferences
		}
		saveMemberStore()
		memberStoreMu.Unlock()
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		// Push real-time settings update to all of this member's connected sessions
		// so that other devices (Android, iOS, other web tabs) update instantly.
		go func(call string, p map[string]interface{}) {
			syncMsg := wsMessage{Type: "member_sync", Data: p}
			data, _ := json.Marshal(syncMsg)
			clientsMu.Lock()
			defer clientsMu.Unlock()
			for c := range clients {
				if c.authenticated && strings.EqualFold(c.callsign, call) {
					select {
					case c.send <- data:
					default:
					}
				}
			}
		}(m.Callsign, prefs)
		return
	}
	http.Error(w, "method not allowed", 405)
}

// GET /api/members/callsigns - public list of registered member callsigns.
// Only exposes callsigns, no personal data. Used by clients to badge members.
func handleMembersCallsigns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	memberStoreMu.RLock()
	calls := make([]string, 0, len(memberStore.Members))
	for _, m := range memberStore.Members {
		if m.Callsign != "" {
			calls = append(calls, strings.ToUpper(m.Callsign))
		}
	}
	memberStoreMu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=300") // 5-min client cache
	json.NewEncoder(w).Encode(calls)
}

// POST /api/member/message/send - deliver a message to another member directly
// via the server WebSocket, bypassing APRS-IS entirely.
// Use when APRS delivery fails and the recipient is a known APRS Net member.
// Auth: X-Member-Token header. Body: {"to":"CALLSIGN","text":"message"}.
func handleMemberMessageSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	m := getMemberFromRequest(r)
	if m == nil {
		w.WriteHeader(401)
		json.NewEncoder(w).Encode(map[string]string{"error": "not logged in"})
		return
	}
	var req struct {
		To   string `json:"to"`
		Text string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, 400)
		return
	}
	req.To = strings.ToUpper(strings.TrimSpace(req.To))
	if req.To == "" || len(strings.TrimSpace(req.Text)) == 0 {
		http.Error(w, `{"error":"to and text required"}`, 400)
		return
	}
	if len(req.Text) > 67 {
		req.Text = req.Text[:67]
	}

	// Verify recipient is a registered member
	memberStoreMu.RLock()
	recipientExists := false
	for _, mem := range memberStore.Members {
		if strings.ToUpper(mem.Callsign) == req.To {
			recipientExists = true
			break
		}
	}
	memberStoreMu.RUnlock()
	if !recipientExists {
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]string{"error": "recipient is not an APRS Net member"})
		return
	}

	// Build a synthetic APRS message packet the client parses normally.
	// Destination padded to 9 chars per APRS spec.
	msgId := fmt.Sprintf("%02d", time.Now().Unix()%100)
	dest := fmt.Sprintf("%-9s", req.To)
	rawPacket := fmt.Sprintf("%s>APNUK,TCPIP*::%-9s:%s{%s",
		m.Callsign, req.To, req.Text, msgId)
	_ = dest // suppress lint

	// Broadcast to all WS sessions authenticated as the recipient
	data, _ := json.Marshal(wsMessage{Type: "rx", Packet: rawPacket})
	delivered := 0
	clientsMu.Lock()
	for c := range clients {
		if c.authenticated && strings.ToUpper(c.callsign) == req.To {
			select {
			case c.send <- data:
				delivered++
			default:
			}
		}
	}
	clientsMu.Unlock()

	// Store for both recipient (direction="in") and sender (direction="out").
	// storeMessageForMember also pushes the packet to sender's other sessions.
	go storeMessageForMember(req.To, m.Callsign, req.Text, rawPacket, time.Now().Unix())

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":        true,
		"delivered": delivered,
	})
}

func formatAPRSPosition(lat, lon float64) (string, string) {
	latNS := "N"
	if lat < 0 {
		latNS = "S"
		lat = -lat
	}
	lonEW := "E"
	if lon < 0 {
		lonEW = "W"
		lon = -lon
	}
	latDeg := int(lat)
	latMin := (lat - float64(latDeg)) * 60
	lonDeg := int(lon)
	lonMin := (lon - float64(lonDeg)) * 60
	return fmt.Sprintf("%02d%05.2f%s", latDeg, latMin, latNS),
		fmt.Sprintf("%03d%05.2f%s", lonDeg, lonMin, lonEW)
}

// storeMessageForMember stores an incoming APRS message for a registered member
// and a "sent" copy for the sender if they are also a registered member.
// Calling this once handles both parties.
func storeMessageForMember(to, from, text, raw string, ts int64) {
	toUpper := strings.ToUpper(strings.TrimSpace(to))
	fromUpper := strings.ToUpper(strings.TrimSpace(from))
	memberStoreMu.Lock()
	defer memberStoreMu.Unlock()

	findMemberCall := func(search string) string {
		base := search
		if i := strings.Index(search, "-"); i > 0 {
			base = search[:i]
		}
		for _, m := range memberStore.Members {
			for _, c := range m.Callsigns {
				cu := strings.ToUpper(c)
				if cu == search || strings.ToUpper(strings.SplitN(cu, "-", 2)[0]) == base {
					return strings.ToUpper(m.Callsign)
				}
			}
		}
		return ""
	}

	appendMsg := func(bucket, direction string, read bool) {
		if bucket == "" {
			return
		}
		msg := StoredMessage{
			ID: generateID(), From: from, To: to,
			Text: text, Ts: ts, Read: read, Raw: raw,
			Direction: direction,
		}
		memberStore.Messages[bucket] = append(memberStore.Messages[bucket], msg)
		if len(memberStore.Messages[bucket]) > 500 {
			memberStore.Messages[bucket] = memberStore.Messages[bucket][len(memberStore.Messages[bucket])-500:]
		}
	}

	// Recipient bucket (direction="in", unread)
	recipientCall := findMemberCall(toUpper)
	appendMsg(recipientCall, "in", false)

	// Sender bucket (direction="out", pre-read)
	senderCall := findMemberCall(fromUpper)
	if senderCall != "" && senderCall != recipientCall {
		appendMsg(senderCall, "out", true)
	}

	saveMemberStore()

	// Web Push notification to the RECIPIENT's registered devices.
	// This fires even when the recipient has no open page/app.
	var recipientID string
	for _, m := range memberStore.Members {
		if strings.ToUpper(m.Callsign) == recipientCall {
			recipientID = m.ID
			break
		}
	}
	if recipientID != "" {
		go pushToMember(recipientID, PushPayload{
			Type:  "message",
			Title: "APRS Message from " + from,
			Body:  text,
			Tag:   "aprs_msg_" + fromUpper,
		})
		// Also deliver to any of the member's online MQTT devices (T-Deck etc.)
		go pushMessageToMemberDevices(recipientID, from, text)
	}

	// Push a real-time WS copy to all of the SENDER's other connected sessions
	// so that other devices (web, desktop) see the sent message immediately.
	if senderCall != "" {
		pkt := wsMessage{Type: "rx", Packet: raw}
		data, _ := json.Marshal(pkt)
		go func() {
			clientsMu.Lock()
			defer clientsMu.Unlock()
			for c := range clients {
				if c.authenticated && strings.ToUpper(c.callsign) == senderCall {
					select {
					case c.send <- data:
					default:
					}
				}
			}
		}()
	}
}

// ─── Offline Message Delivery on Client Connect ───────────────────────────────
// When a recognised member callsign connects (TCP or WebSocket), look for any
// stored offline messages addressed to that callsign and stream them back.
// Marks delivered messages as Read so they're not re-sent next session.
//
// The destination check is loose: an incoming connection from G7XYZ-9 will
// receive messages stored for G7XYZ (base call) as well as G7XYZ-9 (exact)
// and any callsign listed in that member's Callsigns array.

type messageWriter func(packet string)

func forwardOfflineMessagesToCallsign(callsign string, write messageWriter) int {
	callsign = strings.ToUpper(strings.TrimSpace(callsign))
	if callsign == "" {
		return 0
	}
	baseCall := callsign
	if i := strings.Index(callsign, "-"); i > 0 {
		baseCall = callsign[:i]
	}

	memberStoreMu.Lock()
	defer memberStoreMu.Unlock()

	// Find the member - look up by exact callsign or any of their owned callsigns
	var member *Member
	for _, m := range memberStore.Members {
		if strings.EqualFold(m.Callsign, callsign) || strings.EqualFold(m.Callsign, baseCall) {
			member = m
			break
		}
		for _, c := range m.Callsigns {
			if strings.EqualFold(c, callsign) || strings.EqualFold(c, baseCall) {
				member = m
				break
			}
		}
		if member != nil {
			break
		}
	}
	if member == nil {
		return 0
	}

	// Collect unread messages for any of the member's callsigns matching this connection
	var pending []StoredMessage
	var deliveredKeys = make(map[string]bool) // call -> any delivered (so we can mark them)
	for storedCall, msgs := range memberStore.Messages {
		// Should this storedCall be delivered to the connecting callsign?
		// Yes if storedCall == callsign, storedCall == baseCall, or member owns both.
		match := strings.EqualFold(storedCall, callsign) ||
			strings.EqualFold(storedCall, baseCall) ||
			strings.EqualFold(storedCall, member.Callsign)
		if !match {
			for _, c := range member.Callsigns {
				if strings.EqualFold(c, storedCall) {
					match = true
					break
				}
			}
		}
		if !match {
			continue
		}
		for _, m := range msgs {
			if !m.Read {
				pending = append(pending, m)
				deliveredKeys[storedCall] = true
			}
		}
	}
	if len(pending) == 0 {
		return 0
	}

	// Sort oldest first (so they arrive in chronological order)
	for i := 0; i < len(pending); i++ {
		for j := i + 1; j < len(pending); j++ {
			if pending[j].Ts < pending[i].Ts {
				pending[i], pending[j] = pending[j], pending[i]
			}
		}
	}

	// Format and send each message as a standard APRS message packet
	for _, m := range pending {
		var pkt string
		if m.Raw != "" {
			// We have the original raw packet - send it verbatim
			pkt = m.Raw
		} else {
			// Reconstruct: FROM>APAGOR,TCPIP*::TOCALL___:text{msgid}
			// Pad TO call to 9 chars
			toPadded := m.To
			for len(toPadded) < 9 {
				toPadded += " "
			}
			pkt = fmt.Sprintf("%s>APAGOR,TCPIP*::%s:%s", m.From, toPadded, m.Text)
			if m.ID != "" {
				pkt += "{" + m.ID
			}
		}
		write(pkt)
	}

	// Mark them as Read across all matching keys
	for storedCall := range deliveredKeys {
		for i := range memberStore.Messages[storedCall] {
			if !memberStore.Messages[storedCall][i].Read {
				memberStore.Messages[storedCall][i].Read = true
			}
		}
	}
	saveMemberStore()

	log.Printf("Forwarded %d offline messages to %s on connect", len(pending), callsign)
	return len(pending)
}

// ═══════════════════════════════════════════════════════════════════════════════
// ADMIN FEATURES: Member Mgmt, Ban List, Backup/Restore, MOTD, Audit Log
// ═══════════════════════════════════════════════════════════════════════════════

const banFile = "bans.json"
const motdFile = "motd.json"
const auditFile = "audit.log"

// ── Ban list ────────────────────────────────────────────────────────────────

type BanEntry struct {
	Callsign string `json:"callsign"` // exact or with wildcard *
	Reason   string `json:"reason"`
	Added    int64  `json:"added"`
	AddedBy  string `json:"added_by"`
}

var (
	banList   []BanEntry
	banListMu sync.RWMutex
)

func loadBanList() {
	banListMu.Lock()
	defer banListMu.Unlock()
	banList = []BanEntry{}
	data, err := os.ReadFile(banFile)
	if err != nil {
		return
	}
	json.Unmarshal(data, &banList)
	log.Printf("Loaded %d ban entries", len(banList))
}

func saveBanList() {
	data, _ := json.MarshalIndent(banList, "", "  ")
	os.WriteFile(banFile, data, 0644)
}

// isCallsignBanned: returns reason if banned, empty string if not
func isCallsignBanned(call string) string {
	call = strings.ToUpper(strings.TrimSpace(call))
	banListMu.RLock()
	defer banListMu.RUnlock()
	for _, b := range banList {
		pat := strings.ToUpper(b.Callsign)
		if pat == call {
			return b.Reason
		}
		if strings.HasSuffix(pat, "*") {
			prefix := strings.TrimSuffix(pat, "*")
			if strings.HasPrefix(call, prefix) {
				return b.Reason
			}
		}
	}
	return ""
}

// ── MOTD ───────────────────────────────────────────────────────────────────

type MOTD struct {
	Enabled     bool   `json:"enabled"`
	Message     string `json:"message"`
	Level       string `json:"level"` // info, warning, success, error
	Dismissable bool   `json:"dismissable"`
	Updated     int64  `json:"updated"`
}

var (
	motd   MOTD
	motdMu sync.RWMutex
)

func loadMOTD() {
	motdMu.Lock()
	defer motdMu.Unlock()
	data, err := os.ReadFile(motdFile)
	if err != nil {
		motd = MOTD{}
		return
	}
	json.Unmarshal(data, &motd)
}

func saveMOTD() {
	data, _ := json.MarshalIndent(motd, "", "  ")
	os.WriteFile(motdFile, data, 0644)
}

// ── Audit log ──────────────────────────────────────────────────────────────

type AuditEntry struct {
	Ts      int64  `json:"ts"`
	Actor   string `json:"actor"`  // who did it (admin or "system")
	Action  string `json:"action"` // e.g. "member.delete", "ban.add"
	Target  string `json:"target"` // affected entity
	Details string `json:"details,omitempty"`
	IP      string `json:"ip,omitempty"`
}

var auditMu sync.Mutex

// Router for /api/admin/members/{id} and /api/admin/members/{id}/messages
func handleAdminMemberRouter(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/admin/members/")
	if path == "" {
		http.Error(w, "no id", 400)
		return
	}
	if strings.HasSuffix(path, "/messages") {
		handleAdminMemberMessages(w, r)
		return
	}
	handleAdminMember(w, r)
}

func auditLog(actor, action, target, details, ip string) {
	auditMu.Lock()
	defer auditMu.Unlock()
	entry := AuditEntry{
		Ts: time.Now().Unix(), Actor: actor, Action: action,
		Target: target, Details: details, IP: ip,
	}
	data, _ := json.Marshal(entry)
	f, err := os.OpenFile(auditFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	f.Write(append(data, '\n'))
}

func readAuditLog(lines int) []AuditEntry {
	data, err := os.ReadFile(auditFile)
	if err != nil {
		return []AuditEntry{}
	}
	parts := strings.Split(strings.TrimSpace(string(data)), "\n")
	if lines > 0 && len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	entries := []AuditEntry{}
	for _, line := range parts {
		if line == "" {
			continue
		}
		var e AuditEntry
		if err := json.Unmarshal([]byte(line), &e); err == nil {
			entries = append(entries, e)
		}
	}
	// Reverse so newest first
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	return entries
}

// ── HTTP Handlers ──────────────────────────────────────────────────────────

func getClientIP(r *http.Request) string {
	if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
		return strings.SplitN(xf, ",", 2)[0]
	}
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}

// GET /api/admin/members
func handleAdminMembers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	memberStoreMu.RLock()
	defer memberStoreMu.RUnlock()
	type publicMember struct {
		ID         string   `json:"id"`
		Callsign   string   `json:"callsign"`
		Name       string   `json:"name"`
		Email      string   `json:"email"`
		Callsigns  []string `json:"callsigns"`
		Passcode   int      `json:"passcode"`
		WatchCount int      `json:"watch_count"`
		MsgCount   int      `json:"msg_count"`
		Unread     int      `json:"unread"`
		Created    int64    `json:"created"`
		LastLogin  int64    `json:"last_login"`
		Sessions   int      `json:"sessions"`
	}
	list := []publicMember{}
	for _, m := range memberStore.Members {
		msgs := memberStore.Messages[strings.ToUpper(m.Callsign)]
		unread := 0
		for _, mg := range msgs {
			if !mg.Read {
				unread++
			}
		}
		sessions := 0
		for _, s := range memberStore.Sessions {
			if s.MemberID == m.ID {
				sessions++
			}
		}
		list = append(list, publicMember{
			ID: m.ID, Callsign: m.Callsign, Name: m.Name, Email: m.Email,
			Callsigns: m.Callsigns, Passcode: m.Passcode,
			WatchCount: len(m.Watchlist), MsgCount: len(msgs), Unread: unread,
			Created: m.Created, LastLogin: m.LastLogin, Sessions: sessions,
		})
	}
	json.NewEncoder(w).Encode(list)
}

// PUT /api/admin/members/{id} : update member
// DELETE /api/admin/members/{id} : delete member
func handleAdminMember(w http.ResponseWriter, r *http.Request) {
	ip := getClientIP(r)
	// Extract ID from path
	id := strings.TrimPrefix(r.URL.Path, "/api/admin/members/")
	id = strings.SplitN(id, "/", 2)[0]
	if id == "" {
		http.Error(w, "no id", 400)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	memberStoreMu.Lock()
	defer memberStoreMu.Unlock()
	mem, ok := memberStore.Members[id]
	if !ok {
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
		return
	}

	if r.Method == http.MethodDelete {
		delete(memberStore.Members, id)
		delete(memberStore.Messages, strings.ToUpper(mem.Callsign))
		// Kill all sessions for this member
		for tok, s := range memberStore.Sessions {
			if s.MemberID == id {
				delete(memberStore.Sessions, tok)
			}
		}
		saveMemberStore()
		auditLog("admin", "member.delete", mem.Callsign, mem.ID, ip)
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		return
	}

	if r.Method == http.MethodPut {
		var req struct {
			Name          string   `json:"name"`
			Email         string   `json:"email"`
			Callsigns     []string `json:"callsigns"`
			Watchlist     []string `json:"watchlist"`
			ResetPassword string   `json:"reset_password"` // new password if set
			ForceLogout   bool     `json:"force_logout"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		changes := []string{}
		if req.Name != "" && req.Name != mem.Name {
			mem.Name = req.Name
			changes = append(changes, "name")
		}
		if req.Email != mem.Email {
			mem.Email = req.Email
			changes = append(changes, "email")
		}
		if req.Callsigns != nil {
			mem.Callsigns = req.Callsigns
			changes = append(changes, "callsigns")
		}
		if req.Watchlist != nil {
			mem.Watchlist = req.Watchlist
			changes = append(changes, "watchlist")
		}
		if req.ResetPassword != "" && len(req.ResetPassword) >= 6 {
			mem.Password = hashPassword(req.ResetPassword, mem.Salt)
			changes = append(changes, "password")
			// Force logout when password changes
			req.ForceLogout = true
		}
		if req.ForceLogout {
			for tok, s := range memberStore.Sessions {
				if s.MemberID == id {
					delete(memberStore.Sessions, tok)
				}
			}
			changes = append(changes, "sessions cleared")
		}
		saveMemberStore()
		auditLog("admin", "member.update", mem.Callsign, strings.Join(changes, ","), ip)
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "changes": changes})
		return
	}

	http.Error(w, "method not allowed", 405)
}

// GET /api/admin/members/{id}/messages : view member's stored messages
func handleAdminMemberMessages(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/admin/members/")
	id = strings.TrimSuffix(id, "/messages")
	w.Header().Set("Content-Type", "application/json")
	memberStoreMu.RLock()
	defer memberStoreMu.RUnlock()
	mem, ok := memberStore.Members[id]
	if !ok {
		w.WriteHeader(404)
		return
	}
	msgs := memberStore.Messages[strings.ToUpper(mem.Callsign)]
	if msgs == nil {
		msgs = []StoredMessage{}
	}
	json.NewEncoder(w).Encode(msgs)
}

// GET/POST /api/admin/bans
func handleAdminBans(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	ip := getClientIP(r)
	if r.Method == http.MethodGet {
		banListMu.RLock()
		defer banListMu.RUnlock()
		json.NewEncoder(w).Encode(banList)
		return
	}
	if r.Method == http.MethodPost {
		var req BanEntry
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		req.Callsign = strings.ToUpper(strings.TrimSpace(req.Callsign))
		req.Added = time.Now().Unix()
		req.AddedBy = "admin"
		banListMu.Lock()
		// Update if exists, else append
		updated := false
		for i, b := range banList {
			if strings.ToUpper(b.Callsign) == req.Callsign {
				banList[i] = req
				updated = true
				break
			}
		}
		if !updated {
			banList = append(banList, req)
		}
		saveBanList()
		banListMu.Unlock()
		auditLog("admin", "ban.add", req.Callsign, req.Reason, ip)
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		return
	}
	if r.Method == http.MethodDelete {
		call := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("callsign")))
		if call == "" {
			http.Error(w, "no callsign", 400)
			return
		}
		banListMu.Lock()
		newList := []BanEntry{}
		for _, b := range banList {
			if strings.ToUpper(b.Callsign) != call {
				newList = append(newList, b)
			}
		}
		banList = newList
		saveBanList()
		banListMu.Unlock()
		auditLog("admin", "ban.remove", call, "", ip)
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		return
	}
	http.Error(w, "method not allowed", 405)
}

// GET/POST /api/admin/motd
// GET /api/motd (public)
func handleAdminMOTD(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		motdMu.RLock()
		defer motdMu.RUnlock()
		json.NewEncoder(w).Encode(motd)
		return
	}
	if r.Method == http.MethodPost {
		var req MOTD
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		motdMu.Lock()
		motd = req
		motd.Updated = time.Now().Unix()
		saveMOTD()
		motdMu.Unlock()
		auditLog("admin", "motd.update", "", req.Message[:minLen(req.Message, 50)], getClientIP(r))
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		return
	}
	http.Error(w, "method not allowed", 405)
}

func handlePublicMOTD(w http.ResponseWriter, r *http.Request) {
	motdMu.RLock()
	defer motdMu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	if !motd.Enabled || motd.Message == "" {
		json.NewEncoder(w).Encode(map[string]bool{"enabled": false})
		return
	}
	json.NewEncoder(w).Encode(motd)
}

// GET /api/admin/audit?limit=100
func handleAdminAudit(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}
	entries := readAuditLog(limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

// GET /api/admin/backup : returns zip of all state

// handleAdminUpdate — POST /api/admin/update
// Pulls latest code from GitHub, rebuilds the binary, and restarts the service.
// Requires HTTP Basic Auth (admin credentials). Safe to call from a browser or curl.
// Returns a JSON stream of progress lines as the build runs.
func handleAdminUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	flusher, canFlush := w.(http.Flusher)
	write := func(msg string) {
		fmt.Fprintf(w, "%s\n", msg)
		if canFlush {
			flusher.Flush()
		}
	}

	write("=== APRS Net self-update ===")

	dir := "/opt/aprs-gateway"
	steps := []struct {
		label string
		args  []string
	}{
		{"git pull", []string{"git", "-C", dir, "pull", "origin", "main"}},
		{"go build", []string{"/usr/local/go/bin/go", "build", "-o", dir + "/aprs_server", dir + "/..."}},
		{"restart", []string{"systemctl", "restart", "aprs"}},
	}

	for _, step := range steps {
		write(fmt.Sprintf("--- %s ---", step.label))
		cmd := exec.Command(step.args[0], step.args[1:]...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if len(out) > 0 {
			write(string(out))
		}
		if err != nil {
			write(fmt.Sprintf("ERROR: %v", err))
			return
		}
	}
	write("=== update complete ===")
}

// ─── Member geo-fence alert rules ────────────────────────────────────────────
//
// Rules are stored inside each Member's AlertRules slice and persisted through
// the existing members.json store.  Geofence state (inside/outside) is kept in
// a global in-memory map that resets on server restart — first packet after
// restart silently initialises state without firing an alert.

// MemberAlertRule is a single geo-fence rule owned by a member.
type MemberAlertRule struct {
	ID            int64   `json:"id"`
	Type          string  `json:"type"`           // "geofence_enter" | "geofence_exit"
	WatchCallsign string  `json:"watch_callsign"` // specific callsign or "*" for all
	Lat           float64 `json:"lat"`
	Lon           float64 `json:"lon"`
	RadiusMi      float64 `json:"radius_mi"`
	Name          string  `json:"name"`
}

// geofenceState tracks the last known inside/outside status for each
// (rule_id, watched_callsign) pair.  Key: "<ruleID>:<watchedCall>"
var (
	geofenceStateMu sync.Mutex
	geofenceState   = map[string]int{} // -1=unseen, 0=outside, 1=inside
)

func gfKey(ruleID int64, call string) string {
	return fmt.Sprintf("%d:%s", ruleID, strings.ToUpper(call))
}

// handleAlertRules — GET lists all rules, POST creates a new one.
// Path: /api/member/alert-rules  (protected by memberAuth)
func handleAlertRules(w http.ResponseWriter, r *http.Request) {
	mem := getMemberFromRequest(r)
	if mem == nil {
		http.Error(w, "unauthorized", 401)
		return
	}

	if r.Method == http.MethodGet {
		memberStoreMu.RLock()
		rules := mem.AlertRules
		if rules == nil {
			rules = []MemberAlertRule{}
		}
		memberStoreMu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rules)
		return
	}

	if r.Method == http.MethodPost {
		var rule MemberAlertRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
		if rule.Type != "geofence_enter" && rule.Type != "geofence_exit" {
			http.Error(w, "type must be geofence_enter or geofence_exit", 400)
			return
		}
		if rule.RadiusMi <= 0 {
			rule.RadiusMi = 10
		}
		if rule.WatchCallsign == "" {
			rule.WatchCallsign = "*"
		}
		rule.WatchCallsign = strings.ToUpper(rule.WatchCallsign)

		memberStoreMu.Lock()
		var maxID int64
		for _, existing := range mem.AlertRules {
			if existing.ID > maxID {
				maxID = existing.ID
			}
		}
		rule.ID = maxID + 1
		mem.AlertRules = append(mem.AlertRules, rule)
		saveMemberStore()
		memberStoreMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(rule)
		// Push the updated rule list to all other sessions for this member
		// so Android, iOS, and other web tabs refresh immediately.
		go broadcastGeoFenceSync(mem.Callsign)
		return
	}
	http.Error(w, "method not allowed", 405)
}

// handleAlertRuleDelete — DELETE /api/member/alert-rules/{id}
func handleAlertRuleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "DELETE required", 405)
		return
	}
	mem := getMemberFromRequest(r)
	if mem == nil {
		http.Error(w, "unauthorized", 401)
		return
	}

	idStr := strings.TrimPrefix(r.URL.Path, "/api/member/alert-rules/")
	var id int64
	if _, err := fmt.Sscanf(idStr, "%d", &id); err != nil {
		http.Error(w, "invalid id", 400)
		return
	}

	memberStoreMu.Lock()
	newRules := mem.AlertRules[:0]
	for _, rule := range mem.AlertRules {
		if rule.ID != id {
			newRules = append(newRules, rule)
		}
	}
	mem.AlertRules = newRules
	saveMemberStore()
	memberStoreMu.Unlock()
	w.WriteHeader(http.StatusNoContent)
	// Push updated list to all other sessions for this member
	go broadcastGeoFenceSync(mem.Callsign)
}

// broadcastGeoFenceSync pushes the current alert-rule list for [callsign] to
// every other authenticated WS session for that member, so Android, iOS, and
// other web tabs refresh their geo-fence lists immediately without polling.
func broadcastGeoFenceSync(callsign string) {
	memberStoreMu.RLock()
	var rules []MemberAlertRule
	for _, mem := range memberStore.Members {
		if strings.EqualFold(mem.Callsign, callsign) {
			rules = mem.AlertRules
			break
		}
	}
	if rules == nil {
		rules = []MemberAlertRule{}
	}
	memberStoreMu.RUnlock()

	msg := wsMessage{Type: "geo_fence_sync", Data: rules}
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	clientsMu.Lock()
	defer clientsMu.Unlock()
	for c := range clients {
		if c.authenticated && strings.EqualFold(c.callsign, callsign) {
			select {
			case c.send <- data:
			default:
			}
		}
	}
}

// haversineKm returns the great-circle distance in kilometres between two
// lat/lon coordinates using the Haversine formula.
func haversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371.0
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180)*math.Cos(lat2*math.Pi/180)*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	return R * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// checkGeofenceAlerts is called for every position packet with valid coordinates.
// It evaluates all member geofence rules that match watchCall and fires alerts
// via pushAlertToMember when an enter/exit transition is detected.
func checkGeofenceAlerts(watchCall string, lat, lon float64) {
	typeAlerts := map[string]struct {
		memberCall string
		rule       MemberAlertRule
		distMi     float64
		inside     bool
	}{}

	memberStoreMu.RLock()
	for _, mem := range memberStore.Members {
		for _, rule := range mem.AlertRules {
			wc := strings.ToUpper(rule.WatchCallsign)
			if wc != "*" && wc != strings.ToUpper(watchCall) {
				continue
			}
			distKm := haversineKm(lat, lon, rule.Lat, rule.Lon)
			distMi := distKm * 0.621371
			typeAlerts[gfKey(rule.ID, watchCall)] = struct {
				memberCall string
				rule       MemberAlertRule
				distMi     float64
				inside     bool
			}{mem.Callsign, rule, distMi, distMi <= rule.RadiusMi}
		}
	}
	memberStoreMu.RUnlock()

	geofenceStateMu.Lock()
	for key, cand := range typeAlerts {
		prev, known := geofenceState[key]
		nowInside := btoi(cand.inside)
		geofenceState[key] = nowInside

		if !known {
			continue
		} // first observation — initialise silently

		zoneName := cand.rule.Name
		if zoneName == "" {
			zoneName = "zone"
		}
		var alertMsg string
		switch {
		case cand.rule.Type == "geofence_enter" && prev == 0 && nowInside == 1:
			alertMsg = fmt.Sprintf("%s entered %s (%.1f mi from centre)", watchCall, zoneName, cand.distMi)
		case cand.rule.Type == "geofence_exit" && prev == 1 && nowInside == 0:
			alertMsg = fmt.Sprintf("%s left %s (%.1f mi from centre)", watchCall, zoneName, cand.distMi)
		}
		if alertMsg != "" {
			go pushAlertToMember(cand.memberCall, cand.rule.Type, watchCall, alertMsg)
		}
	}
	geofenceStateMu.Unlock()
}

// pushAlertToMember sends an alert WebSocket frame to all sessions that belong
// to memberCall AND delivers a Web Push notification to any registered devices.
func pushAlertToMember(memberCall, alertType, stationCall, message string) {
	alert := map[string]interface{}{
		"type":       "alert",
		"alert_type": alertType,
		"callsign":   stationCall,
		"message":    message,
	}
	data, _ := json.Marshal(alert)
	clientsMu.Lock()
	for c := range clients {
		if strings.EqualFold(c.callsign, memberCall) {
			select {
			case c.send <- data:
			default:
			}
		}
	}
	clientsMu.Unlock()

	// Also push to any registered Web Push subscriptions for this member
	memberStoreMu.RLock()
	var memberID string
	for _, m := range memberStore.Members {
		if strings.EqualFold(m.Callsign, memberCall) {
			memberID = m.ID
			break
		}
	}
	memberStoreMu.RUnlock()
	if memberID != "" {
		go pushToMember(memberID, PushPayload{
			Type:  alertType,
			Title: stationCall + " — " + alertType,
			Body:  message,
			Tag:   "aprs_alert_" + stationCall,
		})
	}
}

// btoi converts bool to int for the geofenceState map.
func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func handleAdminBackup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="aprs-backup-%s.json"`, time.Now().Format("2006-01-02-150405")))
	backup := map[string]interface{}{
		"version":       AppVersion,
		"timestamp":     time.Now().Unix(),
		"server_config": loadConfigFromFile(),
		"members":       memberStore,
		"bans":          banList,
		"motd":          motd,
		"webhooks":      webhooks,
		"api_keys":      apiKeys,
	}
	json.NewEncoder(w).Encode(backup)
	auditLog("admin", "backup.download", "", "", getClientIP(r))
}

// POST /api/admin/restore : upload backup JSON
func handleAdminRestore(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	body, err := io.ReadAll(r.Body)
	if err != nil || len(body) == 0 {
		http.Error(w, "no body", 400)
		return
	}
	var backup map[string]json.RawMessage
	if err := json.Unmarshal(body, &backup); err != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	restored := []string{}
	if mb, ok := backup["members"]; ok {
		var ms MemberStore
		if json.Unmarshal(mb, &ms) == nil {
			memberStoreMu.Lock()
			memberStore = &ms
			saveMemberStore()
			memberStoreMu.Unlock()
			restored = append(restored, "members")
		}
	}
	if bb, ok := backup["bans"]; ok {
		var bl []BanEntry
		if json.Unmarshal(bb, &bl) == nil {
			banListMu.Lock()
			banList = bl
			saveBanList()
			banListMu.Unlock()
			restored = append(restored, "bans")
		}
	}
	if mm, ok := backup["motd"]; ok {
		var m MOTD
		if json.Unmarshal(mm, &m) == nil {
			motdMu.Lock()
			motd = m
			saveMOTD()
			motdMu.Unlock()
			restored = append(restored, "motd")
		}
	}
	auditLog("admin", "backup.restore", "", strings.Join(restored, ","), getClientIP(r))
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "restored": restored})
}

// =============================================================================
// AIS live ship feed via aisstream.io WebSocket
// Subscribes to NW European / UK waters, converts position reports to
// HistoryPacket, and broadcasts them to all WS clients as "rx" messages so
// every connected client (web and mobile) receives live ship positions.
// =============================================================================

type aisMessage struct {
	MessageType string `json:"MessageType"`
	MetaData    struct {
		MMSI       int64   `json:"MMSI"`
		MMSIString string  `json:"MMSI_String"`
		ShipName   string  `json:"ShipName"`
		Latitude   float64 `json:"latitude"`
		Longitude  float64 `json:"longitude"`
	} `json:"MetaData"`
	Message struct {
		PositionReport *struct {
			Latitude         float64 `json:"Latitude"`
			Longitude        float64 `json:"Longitude"`
			SpeedOverGround  float32 `json:"SpeedOverGround"`
			CourseOverGround float32 `json:"CourseOverGround"`
			TrueHeading      int     `json:"TrueHeading"`
		} `json:"PositionReport"`
	} `json:"Message"`
}

func runAISStream() {
	const reconnectBase = 5 * time.Second
	const reconnectMax = 120 * time.Second
	delay := reconnectBase

	for {
		config.RLock()
		key := config.AISStreamKey
		config.RUnlock()

		if key == "" {
			time.Sleep(30 * time.Second)
			continue
		}

		err := connectAIS(key)
		if err != nil {
			log.Printf("[AIS] disconnected: %v – retrying in %v", err, delay)
		}
		time.Sleep(delay)
		if delay < reconnectMax {
			delay = delay * 2
			if delay > reconnectMax {
				delay = reconnectMax
			}
		}
	}
}

func connectAIS(apiKey string) error {
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.Dial("wss://stream.aisstream.io/v0/stream", nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// Subscribe to UK + NW European coastal waters
	sub := map[string]interface{}{
		"APIKey": apiKey,
		"BoundingBoxes": []interface{}{
			[]interface{}{
				[]float64{48.0, -12.0},
				[]float64{62.0, 5.0},
			},
		},
		"FilterMessageTypes": []string{"PositionReport"},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	log.Printf("[AIS] connected to aisstream.io")

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		var msg aisMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		if msg.MessageType != "PositionReport" || msg.Message.PositionReport == nil {
			continue
		}

		pr := msg.Message.PositionReport
		lat := pr.Latitude
		lon := pr.Longitude

		// Sanity-check: valid coords and not the "not available" sentinel 91/181
		if lat < -90 || lat > 90 || lon < -180 || lon > 180 || lat == 0 || lon == 0 {
			continue
		}
		if lat >= 91 || lon >= 181 {
			continue
		}

		call := msg.MetaData.MMSIString
		if call == "" {
			call = fmt.Sprintf("%d", msg.MetaData.MMSI)
		}
		name := strings.TrimSpace(msg.MetaData.ShipName)
		comment := name

		hp := HistoryPacket{
			Timestamp: time.Now().Unix(),
			Callsign:  call,
			Lat:       lat,
			Lon:       lon,
			Symbol:    "/s",
			Raw:       fmt.Sprintf("%s>AIS,TCPIP*:!AIS", call),
		}
		if comment != "" && comment != "UNKNOWN" {
			hp.Raw = fmt.Sprintf("%s>AIS,TCPIP*:!AIS %s", call, comment)
		}

		wsMsg := wsMessage{Type: "rx", Data: hp}
		data, err := json.Marshal(wsMsg)
		if err != nil {
			continue
		}
		clientsMu.Lock()
		for c := range clients {
			select {
			case c.send <- data:
			default:
			}
		}
		clientsMu.Unlock()
	}
}

// Helpers
func minLen(s string, n int) int {
	if len(s) < n {
		return len(s)
	}
	return n
}
func loadConfigFromFile() interface{} {
	data, err := os.ReadFile("server_config.json")
	if err != nil {
		return nil
	}
	var c interface{}
	json.Unmarshal(data, &c)
	return c
}

//go:embed tocalls.json
var embeddedTocallsJSON []byte

// ─── Ecowitt Weather Station Beaconing ───────────────────────────────────────

// ecowittReading holds the fields needed for an APRS WX packet.
type ecowittReading struct {
	WindDirDeg  int
	WindMph     float64
	GustMph     float64
	TempF       float64
	Rain1hMm    float64
	Rain24hMm   float64
	RainDailyMm float64
	HumidityPct int
	PressureHpa float64
	SolarWm2    *float64
	UvIndex     *int
	Lightning   *int
}

// fetchEcowittReading calls the Ecowitt real-time API and returns parsed data.
// Units requested: °F (temp_unitid=2), hPa (pressure_unitid=3),
// mph (wind_speed_unitid=6), mm (rainfall_unitid=12).
func fetchEcowittReading(appKey, apiKey, mac string, relPressure bool) (*ecowittReading, error) {
	pressureKey := "relative"
	if !relPressure {
		pressureKey = "absolute"
	}
	url := fmt.Sprintf(
		"https://api.ecowitt.net/api/v3/device/real_time?application_key=%s&api_key=%s&mac=%s"+
			"&call_back=all&temp_unitid=2&pressure_unitid=3&wind_speed_unitid=6&rainfall_unitid=12",
		appKey, apiKey, mac,
	)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var raw map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	if code, _ := raw["code"].(float64); code != 0 {
		msg, _ := raw["msg"].(string)
		return nil, fmt.Errorf("ecowitt api error %v: %s", code, msg)
	}
	data, _ := raw["data"].(map[string]interface{})
	if data == nil {
		return nil, fmt.Errorf("ecowitt: empty data")
	}

	getVal := func(section, key string) float64 {
		s, _ := data[section].(map[string]interface{})
		if s == nil {
			return 0
		}
		f, _ := s[key].(map[string]interface{})
		if f == nil {
			return 0
		}
		v, _ := f["value"].(string)
		n, _ := strconv.ParseFloat(v, 64)
		return n
	}
	r := &ecowittReading{
		WindDirDeg:  int(getVal("wind", "wind_direction")),
		WindMph:     getVal("wind", "wind_speed"),
		GustMph:     getVal("wind", "wind_gust"),
		TempF:       getVal("outdoor", "temperature"),
		Rain1hMm:    getVal("rainfall", "1_hour"),
		Rain24hMm:   getVal("rainfall", "24_hours"),
		RainDailyMm: getVal("rainfall", "daily"),
		HumidityPct: int(getVal("outdoor", "humidity")),
	}
	// Pressure: data.pressure.relative.value is a plain string — read directly.
	if pSec, ok := data["pressure"].(map[string]interface{}); ok {
		if pObj, ok := pSec[pressureKey].(map[string]interface{}); ok {
			if v, ok := pObj["value"].(string); ok {
				r.PressureHpa, _ = strconv.ParseFloat(v, 64)
			}
		}
	}

	if solarSec, ok := data["solar_and_uvi"].(map[string]interface{}); ok {
		if sv, ok := solarSec["solar"].(map[string]interface{}); ok {
			if v, _ := sv["value"].(string); v != "" {
				f, _ := strconv.ParseFloat(v, 64)
				r.SolarWm2 = &f
			}
		}
		if uv, ok := solarSec["uvi"].(map[string]interface{}); ok {
			if v, _ := uv["value"].(string); v != "" {
				f, _ := strconv.ParseFloat(v, 64)
				i := int(f)
				r.UvIndex = &i
			}
		}
	}
	if lightningSec, ok := data["lightning"].(map[string]interface{}); ok {
		if lc, ok := lightningSec["count"].(map[string]interface{}); ok {
			if v, _ := lc["value"].(string); v != "" {
				f, _ := strconv.ParseFloat(v, 64)
				i := int(f)
				r.Lightning = &i
			}
		}
	}
	return r, nil
}

// mmToHundredths converts mm to 1/100 inch, capped at 999.
func mmToHundredths(mm float64) int {
	v := int(mm * 3.93701)
	if v < 0 {
		v = 0
	}
	if v > 999 {
		v = 999
	}
	return v
}

// formatWxPacket builds the full APRS WX packet string.
func formatWxPacket(callsign string, ssid int, lat, lon float64, r *ecowittReading,
	solar, uv, lightning bool, comment string) string {

	ts := time.Now().UTC().Format("021504") + "z"

	// Position
	latNS, lonEW := "N", "E"
	latAbs, lonAbs := lat, lon
	if lat < 0 {
		latNS = "S"
		latAbs = -lat
	}
	if lon < 0 {
		lonEW = "W"
		lonAbs = -lon
	}
	latDeg := int(latAbs)
	latMin := (latAbs - float64(latDeg)) * 60.0
	lonDeg := int(lonAbs)
	lonMin := (lonAbs - float64(lonDeg)) * 60.0
	pos := fmt.Sprintf("%02d%05.2f%s/%03d%05.2f%s",
		latDeg, latMin, latNS, lonDeg, lonMin, lonEW)

	// Temperature: signed 3-digit (APRS spec allows - prefix)
	tempStr := fmt.Sprintf("%03d", int(r.TempF))
	if r.TempF < 0 {
		tempStr = fmt.Sprintf("-%02d", int(-r.TempF))
	}

	// Humidity: 00 = 100%
	humStr := fmt.Sprintf("%02d", r.HumidityPct)
	if r.HumidityPct >= 100 {
		humStr = "00"
	}

	wx := fmt.Sprintf("_%03d/%03dg%03dt%sr%03dp%03dP%03dh%sb%05d",
		r.WindDirDeg%360,
		int(r.WindMph+0.5),
		int(r.GustMph+0.5),
		tempStr,
		mmToHundredths(r.Rain1hMm),
		mmToHundredths(r.Rain24hMm),
		mmToHundredths(r.RainDailyMm),
		humStr,
		int(r.PressureHpa*10+0.5),
	)

	// Optional comment suffixes
	var parts []string
	if comment != "" {
		parts = append(parts, comment)
	}
	if solar && r.SolarWm2 != nil {
		parts = append(parts, fmt.Sprintf("L%d", int(*r.SolarWm2+0.5)))
	}
	if uv && r.UvIndex != nil {
		parts = append(parts, fmt.Sprintf("UV%d", *r.UvIndex))
	}
	if lightning && r.Lightning != nil {
		parts = append(parts, fmt.Sprintf("LS%d", *r.Lightning))
	}
	commentStr := ""
	if len(parts) > 0 {
		commentStr = " " + strings.Join(parts, " ")
	}

	src := fmt.Sprintf("%s-%d>APRS,TCPIP*", strings.ToUpper(callsign), ssid)
	return fmt.Sprintf("%s:@%s%s%s%s", src, ts, pos, wx, commentStr)
}

// weatherBeaconLoop fires every minute, checks each member's WX preferences,
// and beacons when their interval has elapsed.
func weatherBeaconLoop() {
	// memberID → last beacon time
	lastBeacon := make(map[string]time.Time)
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		memberStoreMu.RLock()
		members := make([]*Member, 0, len(memberStore.Members))
		for _, m := range memberStore.Members {
			members = append(members, m)
		}
		memberStoreMu.RUnlock()

		for _, m := range members {
			p := m.Preferences
			if p == nil {
				continue
			}
			enabled, _ := p["wx_enabled"].(bool)
			if !enabled {
				continue
			}
			appKey, _ := p["wx_app_key"].(string)
			apiKey, _ := p["wx_api_key"].(string)
			mac, _ := p["wx_mac"].(string)
			if appKey == "" || apiKey == "" || mac == "" {
				continue
			}

			interval := 30
			if iv, ok := p["wx_interval"].(float64); ok && iv > 0 {
				interval = int(iv)
			}
			if last, ok := lastBeacon[m.ID]; ok {
				if time.Since(last) < time.Duration(interval)*time.Minute {
					continue
				}
			}

			// Fetch and beacon
			relPressure := true
			if v, ok := p["wx_pressure"].(string); ok && v == "absolute" {
				relPressure = false
			}
			reading, err := fetchEcowittReading(appKey, apiKey, mac, relPressure)
			if err != nil {
				log.Printf("WxBeacon [%s]: fetch error: %v", m.Callsign, err)
				continue
			}

			ssid := 13
			if v, ok := p["wx_ssid"].(float64); ok && v >= 1 && v <= 15 {
				ssid = int(v)
			}
			lat, _ := p["wx_lat"].(float64)
			lon, _ := p["wx_lon"].(float64)
			solar, _ := p["wx_solar"].(bool)
			uvOpt, _ := p["wx_uv"].(bool)
			lsOpt, _ := p["wx_lightning"].(bool)
			commentStr, _ := p["wx_comment"].(string)
			if commentStr == "" {
				commentStr = "Ecowitt"
			}

			packet := formatWxPacket(m.Callsign, ssid, lat, lon, reading,
				solar, uvOpt, lsOpt, commentStr)
			routed := injectQConstruct(packet, "qAC")
			if isAllowed(routed) && !isDuplicate(routed) {
				log.Printf("WxBeacon TX [%s]: %s", m.Callsign, routed)
				select {
				case broadcast <- routed:
				default:
					log.Printf("WxBeacon [%s]: broadcast channel full, dropping", m.Callsign)
				}
				select {
				case upstreamOut <- routed:
				default:
				}
			}
			lastBeacon[m.ID] = time.Now()
		}
	}
}

// POST /api/server/register — remote APRS Net gateway phones home.
var (
	serverRegMu       sync.Mutex
	serverRegLastSeen = map[string]int64{}
)

func handleServerRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	ip := getClientIP(r)
	now := time.Now().Unix()
	serverRegMu.Lock()
	if now-serverRegLastSeen[ip] < 300 {
		serverRegMu.Unlock()
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "cached": true})
		return
	}
	serverRegLastSeen[ip] = now
	serverRegMu.Unlock()
	var req struct {
		Domain       string `json:"domain"`
		Callsign     string `json:"callsign"`
		ServerName   string `json:"server_name"`
		Version      string `json:"version"`
		StationCount int    `json:"station_count"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	memberStoreMu.Lock()
	registeredAt := now
	if ex := memberStore.KnownServers[ip]; ex != nil {
		registeredAt = ex.RegisteredAt
	}
	memberStore.KnownServers[ip] = &RegisteredServer{
		IP: ip, Domain: req.Domain,
		Callsign:   strings.ToUpper(req.Callsign),
		ServerName: req.ServerName, Version: req.Version,
		StationCount: req.StationCount,
		RegisteredAt: registeredAt, LastSeenAt: now,
	}
	saveMemberStore()
	memberStoreMu.Unlock()
	log.Printf("server.register ip=%s domain=%s call=%s", ip, req.Domain, req.Callsign)
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "ip": ip})
}

// GET /api/admin/servers — all known APRS Net gateways, newest first.
func handleAdminServers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	memberStoreMu.RLock()
	list := make([]*RegisteredServer, 0, len(memberStore.KnownServers))
	for _, s := range memberStore.KnownServers {
		list = append(list, s)
	}
	memberStoreMu.RUnlock()
	sort.Slice(list, func(i, j int) bool { return list[i].LastSeenAt > list[j].LastSeenAt })
	json.NewEncoder(w).Encode(list)
}

// handleWxTest is a thin server-side proxy for the Ecowitt test-connection call.
// It avoids CORS issues from the browser and keeps credentials off the network
// as plaintext query parameters in browser devtools.
// POST /api/member/wx_test   body: {"app_key":"…","api_key":"…","mac":"…"}
func handleWxTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	m := getMemberFromRequest(r)
	if m == nil {
		w.WriteHeader(401)
		json.NewEncoder(w).Encode(map[string]string{"error": "not logged in"})
		return
	}
	var req struct {
		AppKey string `json:"app_key"`
		ApiKey string `json:"api_key"`
		Mac    string `json:"mac"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AppKey == "" {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid request"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	reading, err := fetchEcowittReading(req.AppKey, req.ApiKey, req.Mac, true)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ok": false, "error": err.Error(),
		})
		return
	}
	summary := fmt.Sprintf("%.1f°F | %d%% RH | %.1f hPa | wind %d° @ %.0f mph",
		reading.TempF, reading.HumidityPct, reading.PressureHpa,
		reading.WindDirDeg, reading.WindMph)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": true, "summary": summary,
	})
}
