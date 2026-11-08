// Package dashboard is a small server-rendered administrative dashboard: the
// nodes, the services and their replicas, health, and the recent events.
//
// It is a client of the administrative API, not a second way into the store. It
// renders what GET /v1/nodes, /v1/services, /v1/workloads and /v1/events return,
// by calling the API's own handler in process with the admin token, so it shows
// exactly what an API client would see and cannot show anything the API would
// refuse. It changes nothing: every page is a GET.
//
// Browsers cannot send a bearer header on a link, so the dashboard has a login
// page where the admin token is entered once and exchanged for a signed, expiring,
// HttpOnly, SameSite=Strict cookie. The cookie holds no secret; it is an expiry
// time and an HMAC of it under a key derived from the token, so it is valid on
// every manager that shares the token and dies when the token is rotated. A
// request with a correct "Authorization: Bearer" header is accepted as well,
// for scripts and for a proxy that adds it.
package dashboard

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed templates/*.html static/style.css
var assets embed.FS

const (
	cookieName        = "dockfleet_session"
	minTokenLength    = 16
	defaultSessionTTL = 8 * time.Hour
	defaultRefresh    = 10 * time.Second
	maxLoginBody      = 4 << 10
	maxAPIBody        = 16 << 20
)

// Config configures a Dashboard.
type Config struct {
	// API is the administrative API the dashboard reads through. It must
	// authenticate with "Authorization: Bearer <Token>".
	API http.Handler
	// Token is the admin token: what logs an administrator in, and what the
	// dashboard presents to the API. At least 16 characters.
	Token  string
	Logger *slog.Logger
	// Audit, when set, records a login or logout in the audit trail.
	Audit func(ctx context.Context, action, message string, details map[string]string)
	// SessionTTL is how long a login lasts; zero means 8 hours.
	SessionTTL time.Duration
	// Refresh is how often a page reloads itself; zero means 10 seconds and a
	// negative value turns it off.
	Refresh time.Duration
	// Now defaults to time.Now; tests set it.
	Now func() time.Time
}

// Dashboard is the http.Handler of the pages under /dashboard.
type Dashboard struct {
	cfg     Config
	key     []byte
	pages   map[string]*template.Template
	mux     *http.ServeMux
	limiter *limiter
}

// New builds the dashboard. It fails if the API is missing, the token is too
// short to be a credential, or the templates do not parse.
func New(cfg Config) (*Dashboard, error) {
	switch {
	case cfg.API == nil:
		return nil, errors.New("dashboard: an API handler is required")
	case len(cfg.Token) < minTokenLength:
		return nil, fmt.Errorf("dashboard: the admin token must be at least %d characters", minTokenLength)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = defaultSessionTTL
	}
	if cfg.Refresh == 0 {
		cfg.Refresh = defaultRefresh
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	d := &Dashboard{cfg: cfg, mux: http.NewServeMux(), limiter: newLimiter(cfg.Now)}
	mac := hmac.New(sha256.New, []byte(cfg.Token))
	mac.Write([]byte("dockfleet dashboard session v1"))
	d.key = mac.Sum(nil)

	var err error
	if d.pages, err = d.parsePages(); err != nil {
		return nil, err
	}

	d.mux.HandleFunc("GET /dashboard/static/style.css", d.style)
	d.mux.HandleFunc("GET /dashboard/login", d.loginForm)
	d.mux.HandleFunc("POST /dashboard/login", d.login)
	d.mux.HandleFunc("POST /dashboard/logout", d.logout)
	for _, p := range []string{"GET /dashboard", "GET /dashboard/{$}"} {
		d.mux.Handle(p, d.auth(d.overview))
	}
	d.mux.Handle("GET /dashboard/services/{name}", d.auth(d.service))
	d.mux.Handle("GET /dashboard/nodes/{id}", d.auth(d.node))
	d.mux.Handle("GET /dashboard/events", d.auth(d.events))
	return d, nil
}

// ServeHTTP serves the dashboard's pages. Every response carries the headers
// that keep the pages from being framed, cached or given scripts.
func (d *Dashboard) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
	// No script at all, and styles only from the dashboard's own stylesheet.
	h.Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
	d.mux.ServeHTTP(w, r)
}

func (d *Dashboard) style(w http.ResponseWriter, _ *http.Request) {
	css, err := assets.ReadFile("static/style.css")
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_, _ = w.Write(css)
}

// authResult is how a request stands with the dashboard.
type authResult int

const (
	notLoggedIn authResult = iota
	loggedIn
	badCredentials // a bearer header was sent and it is wrong
)

func (d *Dashboard) authenticate(r *http.Request) authResult {
	if h := r.Header.Get("Authorization"); h != "" {
		if subtle.ConstantTimeCompare([]byte(h), []byte("Bearer "+d.cfg.Token)) == 1 {
			return loggedIn
		}
		return badCredentials
	}
	if c, err := r.Cookie(cookieName); err == nil && d.validSession(c.Value) {
		return loggedIn
	}
	return notLoggedIn
}

// auth lets a request through to a page only if it is logged in. A browser that
// is not is sent to the login page; a client that sent the wrong bearer token
// gets a plain 401, not a redirect it cannot follow.
func (d *Dashboard) auth(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch d.authenticate(r) {
		case loggedIn:
			next(w, r)
		case badCredentials:
			d.cfg.Logger.WarnContext(r.Context(), "dashboard request with invalid credentials", "path", r.URL.Path, "remote", remoteHost(r))
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
		default:
			http.Redirect(w, r, "/dashboard/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
		}
	})
}

// session cookie: "v1.<expiry>.<hex hmac of "v1.<expiry>">".
func (d *Dashboard) sign(expiry string) string {
	mac := hmac.New(sha256.New, d.key)
	mac.Write([]byte("v1." + expiry))
	return hex.EncodeToString(mac.Sum(nil))
}

func (d *Dashboard) newSession() (value string, expires time.Time) {
	expires = d.cfg.Now().Add(d.cfg.SessionTTL)
	expiry := strconv.FormatInt(expires.Unix(), 10)
	return "v1." + expiry + "." + d.sign(expiry), expires
}

func (d *Dashboard) validSession(value string) bool {
	parts := strings.Split(value, ".")
	if len(parts) != 3 || parts[0] != "v1" {
		return false
	}
	expiry, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || !d.cfg.Now().Before(time.Unix(expiry, 0)) {
		return false
	}
	return hmac.Equal([]byte(parts[2]), []byte(d.sign(parts[1])))
}

func (d *Dashboard) setSession(w http.ResponseWriter, r *http.Request) {
	value, expires := d.newSession()
	http.SetCookie(w, &http.Cookie{
		Name: cookieName, Value: value, Path: "/dashboard", Expires: expires,
		MaxAge: int(d.cfg.SessionTTL / time.Second), HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode,
	})
}

func (d *Dashboard) clearSession(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name: cookieName, Value: "", Path: "/dashboard", MaxAge: -1, HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode,
	})
}

func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// sameOrigin refuses a state-changing request that a browser says came from
// another site. A request without Origin is not from a browser page.
func sameOrigin(r *http.Request) bool {
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host == r.Host
}

// safeNext is where to go after logging in: a path under /dashboard and nothing
// else, so the login page cannot be made to send an administrator elsewhere.
func safeNext(next string) string {
	ok := next == "/dashboard" || strings.HasPrefix(next, "/dashboard/") || strings.HasPrefix(next, "/dashboard?")
	if !ok || strings.ContainsAny(next, "\\\r\n\x00") || strings.HasPrefix(next, "/dashboard/login") {
		return "/dashboard"
	}
	return next
}

type loginPage struct {
	base
	Next    string
	Message string
}

func (d *Dashboard) loginForm(w http.ResponseWriter, r *http.Request) {
	d.render(w, http.StatusOK, "login", loginPage{base: d.head("Log in", ""), Next: safeNext(r.URL.Query().Get("next"))})
}

func (d *Dashboard) login(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(r) {
		d.cfg.Logger.WarnContext(r.Context(), "dashboard login from another origin refused", "remote", remoteHost(r))
		http.Error(w, "cross-origin request refused", http.StatusForbidden)
		return
	}
	remote := remoteHost(r)
	page := loginPage{base: d.head("Log in", "")}
	if !d.limiter.allow(remote) {
		d.cfg.Logger.WarnContext(r.Context(), "dashboard login throttled", "remote", remote)
		w.Header().Set("Retry-After", strconv.Itoa(int(loginWindow/time.Second)))
		page.Message = "Too many failed attempts. Try again in a minute."
		d.render(w, http.StatusTooManyRequests, "login", page)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxLoginBody)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// Only the form body counts: a token in the URL is not a credential.
	token := r.PostForm.Get("token")
	if subtle.ConstantTimeCompare([]byte(token), []byte(d.cfg.Token)) != 1 {
		d.limiter.fail(remote)
		d.cfg.Logger.WarnContext(r.Context(), "dashboard login refused", "remote", remote)
		page.Next, page.Message = safeNext(r.PostForm.Get("next")), "That token was not accepted."
		d.render(w, http.StatusUnauthorized, "login", page)
		return
	}
	d.limiter.reset(remote)
	d.setSession(w, r)
	d.audit(r, "dashboard.login", "an administrator logged in to the dashboard")
	http.Redirect(w, r, safeNext(r.PostForm.Get("next")), http.StatusSeeOther)
}

func (d *Dashboard) logout(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(r) {
		http.Error(w, "cross-origin request refused", http.StatusForbidden)
		return
	}
	if d.authenticate(r) == loggedIn {
		d.audit(r, "dashboard.logout", "an administrator logged out of the dashboard")
	}
	d.clearSession(w, r)
	http.Redirect(w, r, "/dashboard/login", http.StatusSeeOther)
}

func (d *Dashboard) audit(r *http.Request, action, message string) {
	if d.cfg.Audit != nil {
		d.cfg.Audit(r.Context(), action, message, map[string]string{"remote": remoteHost(r)})
	}
	d.cfg.Logger.InfoContext(r.Context(), "audit", "actor", "admin", "action", action, "remote", remoteHost(r), "outcome", "accepted")
}

// apiError is an answer from the API that was not a success.
type apiError struct {
	Status  int
	Message string
}

func (e *apiError) Error() string { return fmt.Sprintf("the API answered %d: %s", e.Status, e.Message) }

// capture is the response writer the API handler writes into.
type capture struct {
	header http.Header
	code   int
	body   bytes.Buffer
	over   bool
}

func (c *capture) Header() http.Header { return c.header }

func (c *capture) WriteHeader(code int) {
	if c.code == 0 {
		c.code = code
	}
}

func (c *capture) Write(p []byte) (int, error) {
	c.WriteHeader(http.StatusOK)
	if c.body.Len()+len(p) > maxAPIBody {
		c.over = true
		return 0, errors.New("response too large")
	}
	return c.body.Write(p)
}

func newRequestID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "dashboard"
	}
	return "dash-" + hex.EncodeToString(b)
}

// get reads a path of the API as JSON into out. The call carries the page
// request's remote address and its own request ID, so the API's logs and audit
// records name the browser that asked and can be matched to the page.
func (d *Dashboard) get(r *http.Request, path string, out any) error {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	req.Host, req.RemoteAddr = r.Host, r.RemoteAddr
	req.Header.Set("Authorization", "Bearer "+d.cfg.Token)
	req.Header.Set("X-Request-ID", newRequestID())
	rec := &capture{header: http.Header{}}
	d.cfg.API.ServeHTTP(rec, req)
	if rec.code == 0 {
		rec.code = http.StatusOK
	}
	switch {
	case rec.over:
		return &apiError{Status: http.StatusBadGateway, Message: "the response is too large"}
	case rec.code < 200 || rec.code > 299:
		var body struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(rec.body.Bytes(), &body)
		return &apiError{Status: rec.code, Message: body.Error}
	}
	if err := json.Unmarshal(rec.body.Bytes(), out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

// fail answers a page whose data could not be read. It shows the API's own
// message for a 404 and a general one otherwise; details stay in the log.
func (d *Dashboard) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, message := http.StatusBadGateway, "The dashboard could not read the manager's API. Try again in a moment."
	var ae *apiError
	if errors.As(err, &ae) && ae.Status == http.StatusNotFound && ae.Message != "" {
		status, message = http.StatusNotFound, ae.Message
	} else {
		d.cfg.Logger.ErrorContext(r.Context(), "dashboard could not read the API", "path", r.URL.Path, "error", err)
	}
	d.render(w, status, "error", errorPage{base: d.head("Error", "overview"), Status: status, Message: message})
}

// render writes a page. It is built in full first, so a template that fails
// gives an error page and not half of a page.
func (d *Dashboard) render(w http.ResponseWriter, status int, page string, data any) {
	var buf bytes.Buffer
	if err := d.pages[page].ExecuteTemplate(&buf, "layout", data); err != nil {
		d.cfg.Logger.Error("dashboard template failed", "page", page, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

type errorPage struct {
	base
	Status  int
	Message string
}

// failedLogins are the wrong tokens one address may try within loginWindow.
const (
	failedLogins = 10
	loginWindow  = time.Minute
	maxTracked   = 1024
)

type window struct {
	failures int
	until    time.Time
}

// limiter slows down a client that guesses tokens. It remembers failures per
// address for a minute, in a table of bounded size.
type limiter struct {
	now      func() time.Time
	mu       sync.Mutex
	failures map[string]*window
}

func newLimiter(now func() time.Time) *limiter {
	return &limiter{now: now, failures: map[string]*window{}}
}

func (l *limiter) allow(remote string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.failures[remote]
	if w == nil {
		return true
	}
	if !l.now().Before(w.until) {
		delete(l.failures, remote)
		return true
	}
	return w.failures < failedLogins
}

func (l *limiter) fail(remote string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.failures[remote]
	if w == nil || !l.now().Before(w.until) {
		if len(l.failures) >= maxTracked {
			l.failures = map[string]*window{}
		}
		w = &window{until: l.now().Add(loginWindow)}
		l.failures[remote] = w
	}
	w.failures++
}

func (l *limiter) reset(remote string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.failures, remote)
}
