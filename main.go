package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultPort          = "3000"
	defaultDBPath        = "/data/gateway.db"
	defaultUpstreamBase  = "https://api.minimax.io/v1"
	defaultWindowSeconds = 5 * 60 * 60
	defaultRequestLimit  = 1000
)

var defaultModels = []string{
	"MiniMax-M2.7",
	"MiniMax-M2.5",
	"MiniMax-M2.1",
	"MiniMax-M2",
}

type config struct {
	Port           string
	DatabasePath   string
	UpstreamBase   string
	UpstreamAPIKey string
	AdminToken     string
	RequestLimit   int
	WindowSeconds  int
	Models         []string
}

type app struct {
	cfg          config
	db           *sql.DB
	client       *http.Client
	allowed      map[string]struct{}
	models       []string
	limiterMutex sync.Mutex
}

type apiKeyRecord struct {
	ID            string     `json:"id"`
	Alias         string     `json:"alias"`
	Prefix        string     `json:"prefix"`
	Active        bool       `json:"active"`
	RequestLimit  int        `json:"request_limit"`
	WindowSeconds int        `json:"window_seconds"`
	CreatedAt     time.Time  `json:"created_at"`
	LastUsedAt    *time.Time `json:"last_used_at,omitempty"`
}

type adminCreateKeyRequest struct {
	Alias         string `json:"alias"`
	RequestLimit  int    `json:"request_limit"`
	WindowSeconds int    `json:"window_seconds"`
}

type adminCreateKeyResponse struct {
	apiKeyRecord
	APIKey string `json:"api_key"`
}

type upstreamChatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature *float64      `json:"temperature,omitempty"`
	MaxTokens   *int          `json:"max_tokens,omitempty"`
	Stream      bool          `json:"stream,omitempty"`
}

type chatMessage struct {
	Role    string      `json:"role"`
	Content interface{} `json:"content"`
}

type responsesRequest struct {
	Model           string      `json:"model"`
	Input           interface{} `json:"input"`
	Instructions    string      `json:"instructions"`
	Temperature     *float64    `json:"temperature,omitempty"`
	MaxOutputTokens *int        `json:"max_output_tokens,omitempty"`
	Stream          bool        `json:"stream,omitempty"`
}

type upstreamChatResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Role    string      `json:"role"`
			Content interface{} `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

type rateLimitResult struct {
	Allowed   bool
	Limit     int
	Remaining int
	ResetAt   time.Time
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	db, err := sql.Open("sqlite", cfg.DatabasePath)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	if err := initDB(db); err != nil {
		log.Fatalf("init database: %v", err)
	}

	allowed := make(map[string]struct{}, len(cfg.Models))
	for _, model := range cfg.Models {
		allowed[model] = struct{}{}
	}

	svc := &app{
		cfg:     cfg,
		db:      db,
		client:  &http.Client{},
		allowed: allowed,
		models:  cfg.Models,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", svc.handleRoot)
	mux.HandleFunc("/healthz", svc.handleHealth)
	mux.HandleFunc("/health/liveliness", svc.handleHealth)
	mux.HandleFunc("/v1/models", svc.handleModels)
	mux.HandleFunc("/v1/chat/completions", svc.handleChatCompletions)
	mux.HandleFunc("/v1/responses", svc.handleResponses)
	mux.HandleFunc("/admin/keys", svc.handleAdminKeys)
	mux.HandleFunc("/admin/keys/disable", svc.handleDisableKey)

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           loggingMiddleware(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("starting gateway on :%s with %d models", cfg.Port, len(cfg.Models))
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func loadConfig() (config, error) {
	cfg := config{
		Port:           envOrDefault("PORT", defaultPort),
		DatabasePath:   envOrDefault("DATABASE_PATH", defaultDBPath),
		UpstreamBase:   strings.TrimRight(envOrDefault("UPSTREAM_BASE_URL", defaultUpstreamBase), "/"),
		UpstreamAPIKey: strings.TrimSpace(os.Getenv("UPSTREAM_API_KEY")),
		AdminToken:     strings.TrimSpace(os.Getenv("ADMIN_TOKEN")),
		RequestLimit:   intEnvOrDefault("REQUEST_LIMIT", defaultRequestLimit),
		WindowSeconds:  intEnvOrDefault("WINDOW_SECONDS", defaultWindowSeconds),
		Models:         splitModels(envOrDefault("UPSTREAM_MODELS", strings.Join(defaultModels, ","))),
	}

	if cfg.UpstreamAPIKey == "" {
		return cfg, errors.New("UPSTREAM_API_KEY is required")
	}
	if cfg.AdminToken == "" {
		return cfg, errors.New("ADMIN_TOKEN is required")
	}
	if cfg.RequestLimit <= 0 {
		return cfg, errors.New("REQUEST_LIMIT must be greater than 0")
	}
	if cfg.WindowSeconds <= 0 {
		return cfg, errors.New("WINDOW_SECONDS must be greater than 0")
	}
	if len(cfg.Models) == 0 {
		return cfg, errors.New("UPSTREAM_MODELS must contain at least one model")
	}
	return cfg, nil
}

func initDB(db *sql.DB) error {
	statements := []string{
		`PRAGMA journal_mode = WAL;`,
		`CREATE TABLE IF NOT EXISTS api_keys (
			id TEXT PRIMARY KEY,
			alias TEXT NOT NULL,
			prefix TEXT NOT NULL,
			token_hash TEXT NOT NULL UNIQUE,
			active INTEGER NOT NULL DEFAULT 1,
			request_limit INTEGER NOT NULL,
			window_seconds INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			last_used_at INTEGER
		);`,
		`CREATE TABLE IF NOT EXISTS request_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			key_id TEXT NOT NULL,
			requested_at INTEGER NOT NULL,
			FOREIGN KEY (key_id) REFERENCES api_keys(id) ON DELETE CASCADE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_request_events_key_time ON request_events(key_id, requested_at);`,
	}
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) handleRoot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":            "minimax-gateway",
		"message":         "OpenAI-compatible gateway for MiniMax",
		"models_endpoint": "/v1/models",
		"chat_endpoint":   "/v1/chat/completions",
		"responses":       "/v1/responses",
	})
}

func (a *app) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (a *app) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	if _, ok := a.requireAPIKey(w, r); !ok {
		return
	}

	data := make([]map[string]interface{}, 0, len(a.models))
	for _, model := range a.models {
		data = append(data, map[string]interface{}{
			"id":       model,
			"object":   "model",
			"created":  0,
			"owned_by": "minimax-gateway",
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"object": "list",
		"data":   data,
	})
}

func (a *app) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	key, ok := a.requireAPIKey(w, r)
	if !ok {
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "unable to read request body")
		return
	}
	defer r.Body.Close()

	var payload struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if !a.isAllowedModel(payload.Model) {
		writeOpenAIError(w, http.StatusBadRequest, fmt.Sprintf("model %q is not available", payload.Model))
		return
	}

	limit, err := a.consumeRequest(r.Context(), key.ID, key.RequestLimit, key.WindowSeconds)
	if err != nil {
		log.Printf("rate limit error: %v", err)
		writeOpenAIError(w, http.StatusInternalServerError, "unable to apply rate limit")
		return
	}
	a.applyRateLimitHeaders(w, limit)
	if !limit.Allowed {
		a.writeRateLimitExceeded(w, limit)
		return
	}

	a.proxyJSON(r.Context(), w, "/chat/completions", body)
}

func (a *app) handleResponses(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	key, ok := a.requireAPIKey(w, r)
	if !ok {
		return
	}

	var payload responsesRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	defer r.Body.Close()

	if !a.isAllowedModel(payload.Model) {
		writeOpenAIError(w, http.StatusBadRequest, fmt.Sprintf("model %q is not available", payload.Model))
		return
	}
	if payload.Stream {
		writeOpenAIError(w, http.StatusBadRequest, "streaming responses are not supported")
		return
	}

	messages, err := convertResponsesInput(payload)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, err.Error())
		return
	}

	limit, err := a.consumeRequest(r.Context(), key.ID, key.RequestLimit, key.WindowSeconds)
	if err != nil {
		log.Printf("rate limit error: %v", err)
		writeOpenAIError(w, http.StatusInternalServerError, "unable to apply rate limit")
		return
	}
	a.applyRateLimitHeaders(w, limit)
	if !limit.Allowed {
		a.writeRateLimitExceeded(w, limit)
		return
	}

	chatPayload := upstreamChatRequest{
		Model:       payload.Model,
		Messages:    messages,
		Temperature: payload.Temperature,
		MaxTokens:   payload.MaxOutputTokens,
	}

	body, err := json.Marshal(chatPayload)
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "unable to prepare upstream request")
		return
	}

	upstreamResp, status, err := a.roundTripJSON(r.Context(), "/chat/completions", body)
	if err != nil {
		log.Printf("upstream request failed: %v", err)
		writeOpenAIError(w, http.StatusBadGateway, "upstream request failed")
		return
	}
	if status >= http.StatusBadRequest {
		copyHeaders(w.Header(), upstreamResp.Header)
		w.WriteHeader(status)
		_, _ = io.Copy(w, upstreamResp.Body)
		upstreamResp.Body.Close()
		return
	}
	defer upstreamResp.Body.Close()

	var upstreamBody upstreamChatResponse
	if err := json.NewDecoder(upstreamResp.Body).Decode(&upstreamBody); err != nil {
		writeOpenAIError(w, http.StatusBadGateway, "unable to decode upstream response")
		return
	}

	assistantText := ""
	if len(upstreamBody.Choices) > 0 {
		assistantText = flattenContent(upstreamBody.Choices[0].Message.Content)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":         "resp_" + randomHex(12),
		"object":     "response",
		"created_at": time.Now().Unix(),
		"status":     "completed",
		"model":      payload.Model,
		"output": []map[string]interface{}{
			{
				"id":     "msg_" + randomHex(12),
				"type":   "message",
				"status": "completed",
				"role":   "assistant",
				"content": []map[string]interface{}{
					{
						"type":        "output_text",
						"text":        assistantText,
						"annotations": []interface{}{},
					},
				},
			},
		},
		"output_text": assistantText,
		"usage": map[string]interface{}{
			"input_tokens":  upstreamBody.Usage.PromptTokens,
			"output_tokens": upstreamBody.Usage.CompletionTokens,
			"total_tokens":  upstreamBody.Usage.TotalTokens,
		},
	})
}

func (a *app) handleAdminKeys(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if !a.requireAdmin(w, r) {
			return
		}
		keys, err := a.listKeys(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "unable to list keys")
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"data": keys})
	case http.MethodPost:
		if !a.requireAdmin(w, r) {
			return
		}
		var req adminCreateKeyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		defer r.Body.Close()

		alias := strings.TrimSpace(req.Alias)
		if alias == "" {
			writeJSONError(w, http.StatusBadRequest, "alias is required")
			return
		}

		requestLimit := req.RequestLimit
		if requestLimit <= 0 {
			requestLimit = a.cfg.RequestLimit
		}
		windowSeconds := req.WindowSeconds
		if windowSeconds <= 0 {
			windowSeconds = a.cfg.WindowSeconds
		}

		record, key, err := a.createKey(r.Context(), alias, requestLimit, windowSeconds)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "unable to create key")
			return
		}

		writeJSON(w, http.StatusCreated, adminCreateKeyResponse{
			apiKeyRecord: record,
			APIKey:       key,
		})
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func (a *app) handleDisableKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	if !a.requireAdmin(w, r) {
		return
	}

	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	defer r.Body.Close()

	if strings.TrimSpace(req.ID) == "" {
		writeJSONError(w, http.StatusBadRequest, "id is required")
		return
	}

	if err := a.disableKey(r.Context(), req.ID); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "unable to disable key")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "disabled"})
}

func (a *app) requireAPIKey(w http.ResponseWriter, r *http.Request) (apiKeyRecord, bool) {
	token, ok := extractBearer(r.Header.Get("Authorization"))
	if !ok {
		writeOpenAIError(w, http.StatusUnauthorized, "missing bearer token")
		return apiKeyRecord{}, false
	}

	record, err := a.lookupKey(r.Context(), token)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeOpenAIError(w, http.StatusUnauthorized, "invalid API key")
			return apiKeyRecord{}, false
		}
		log.Printf("lookup key error: %v", err)
		writeOpenAIError(w, http.StatusInternalServerError, "unable to validate API key")
		return apiKeyRecord{}, false
	}
	if !record.Active {
		writeOpenAIError(w, http.StatusUnauthorized, "API key is disabled")
		return apiKeyRecord{}, false
	}
	return record, true
}

func (a *app) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	token, ok := extractBearer(r.Header.Get("Authorization"))
	if !ok || token != a.cfg.AdminToken {
		writeJSONError(w, http.StatusUnauthorized, "invalid admin token")
		return false
	}
	return true
}

func (a *app) lookupKey(ctx context.Context, token string) (apiKeyRecord, error) {
	row := a.db.QueryRowContext(ctx, `
		SELECT id, alias, prefix, active, request_limit, window_seconds, created_at, last_used_at
		FROM api_keys
		WHERE token_hash = ?
	`, hashToken(token))
	return scanKey(row)
}

func (a *app) listKeys(ctx context.Context) ([]apiKeyRecord, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT id, alias, prefix, active, request_limit, window_seconds, created_at, last_used_at
		FROM api_keys
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []apiKeyRecord
	for rows.Next() {
		record, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (a *app) createKey(ctx context.Context, alias string, requestLimit int, windowSeconds int) (apiKeyRecord, string, error) {
	id := "key_" + randomHex(12)
	apiKey := "gw_" + randomHex(24)
	prefix := apiKey
	if len(prefix) > 12 {
		prefix = prefix[:12]
	}
	now := time.Now().UTC().Unix()

	_, err := a.db.ExecContext(ctx, `
		INSERT INTO api_keys (id, alias, prefix, token_hash, active, request_limit, window_seconds, created_at)
		VALUES (?, ?, ?, ?, 1, ?, ?, ?)
	`, id, alias, prefix, hashToken(apiKey), requestLimit, windowSeconds, now)
	if err != nil {
		return apiKeyRecord{}, "", err
	}

	record := apiKeyRecord{
		ID:            id,
		Alias:         alias,
		Prefix:        prefix,
		Active:        true,
		RequestLimit:  requestLimit,
		WindowSeconds: windowSeconds,
		CreatedAt:     time.Unix(now, 0).UTC(),
	}
	return record, apiKey, nil
}

func (a *app) disableKey(ctx context.Context, id string) error {
	_, err := a.db.ExecContext(ctx, `UPDATE api_keys SET active = 0 WHERE id = ?`, id)
	return err
}

func (a *app) consumeRequest(ctx context.Context, keyID string, limit int, windowSeconds int) (rateLimitResult, error) {
	a.limiterMutex.Lock()
	defer a.limiterMutex.Unlock()

	now := time.Now().UTC().Unix()
	cutoff := now - int64(windowSeconds)

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return rateLimitResult{}, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM request_events WHERE key_id = ? AND requested_at < ?`, keyID, cutoff); err != nil {
		return rateLimitResult{}, err
	}

	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM request_events WHERE key_id = ? AND requested_at >= ?`, keyID, cutoff).Scan(&count); err != nil {
		return rateLimitResult{}, err
	}

	var oldest sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MIN(requested_at) FROM request_events WHERE key_id = ? AND requested_at >= ?`, keyID, cutoff).Scan(&oldest); err != nil {
		return rateLimitResult{}, err
	}

	if count >= limit {
		resetAt := time.Unix(now+int64(windowSeconds), 0).UTC()
		if oldest.Valid {
			resetAt = time.Unix(oldest.Int64+int64(windowSeconds), 0).UTC()
		}
		return rateLimitResult{
			Allowed:   false,
			Limit:     limit,
			Remaining: 0,
			ResetAt:   resetAt,
		}, nil
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO request_events (key_id, requested_at) VALUES (?, ?)`, keyID, now); err != nil {
		return rateLimitResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE api_keys SET last_used_at = ? WHERE id = ?`, now, keyID); err != nil {
		return rateLimitResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return rateLimitResult{}, err
	}

	resetAt := time.Unix(now+int64(windowSeconds), 0).UTC()
	if oldest.Valid {
		resetAt = time.Unix(oldest.Int64+int64(windowSeconds), 0).UTC()
	}
	return rateLimitResult{
		Allowed:   true,
		Limit:     limit,
		Remaining: limit - count - 1,
		ResetAt:   resetAt,
	}, nil
}

func (a *app) applyRateLimitHeaders(w http.ResponseWriter, limit rateLimitResult) {
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(limit.Limit))
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(limit.Remaining))
	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(limit.ResetAt.Unix(), 10))
}

func (a *app) writeRateLimitExceeded(w http.ResponseWriter, limit rateLimitResult) {
	retryAfter := int64(time.Until(limit.ResetAt).Round(time.Second).Seconds())
	if retryAfter < 1 {
		retryAfter = 1
	}
	w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
	writeOpenAIError(w, http.StatusTooManyRequests, "rate limit exceeded")
}

func (a *app) proxyJSON(ctx context.Context, w http.ResponseWriter, path string, body []byte) {
	resp, status, err := a.roundTripJSON(ctx, path, body)
	if err != nil {
		log.Printf("upstream request failed: %v", err)
		writeOpenAIError(w, http.StatusBadGateway, "upstream request failed")
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(status)
	_, _ = io.Copy(flushWriter{ResponseWriter: w}, resp.Body)
}

func (a *app) roundTripJSON(ctx context.Context, path string, body []byte) (*http.Response, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.UpstreamBase+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+a.cfg.UpstreamAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	return resp, resp.StatusCode, nil
}

func (a *app) isAllowedModel(model string) bool {
	_, ok := a.allowed[model]
	return ok
}

func scanKey(scanner interface {
	Scan(dest ...interface{}) error
}) (apiKeyRecord, error) {
	var record apiKeyRecord
	var active int
	var createdAt int64
	var lastUsed sql.NullInt64
	err := scanner.Scan(
		&record.ID,
		&record.Alias,
		&record.Prefix,
		&active,
		&record.RequestLimit,
		&record.WindowSeconds,
		&createdAt,
		&lastUsed,
	)
	if err != nil {
		return apiKeyRecord{}, err
	}

	record.Active = active == 1
	record.CreatedAt = time.Unix(createdAt, 0).UTC()
	if lastUsed.Valid {
		t := time.Unix(lastUsed.Int64, 0).UTC()
		record.LastUsedAt = &t
	}
	return record, nil
}

func convertResponsesInput(req responsesRequest) ([]chatMessage, error) {
	var messages []chatMessage
	if instructions := strings.TrimSpace(req.Instructions); instructions != "" {
		messages = append(messages, chatMessage{
			Role:    "system",
			Content: instructions,
		})
	}

	converted, err := inputToMessages(req.Input)
	if err != nil {
		return nil, err
	}
	messages = append(messages, converted...)
	if len(messages) == 0 {
		return nil, errors.New("input is required")
	}
	return messages, nil
}

func inputToMessages(input interface{}) ([]chatMessage, error) {
	switch value := input.(type) {
	case string:
		text := strings.TrimSpace(value)
		if text == "" {
			return nil, errors.New("input text is empty")
		}
		return []chatMessage{{Role: "user", Content: text}}, nil
	case []interface{}:
		var messages []chatMessage
		for _, item := range value {
			msgs, err := itemToMessages(item)
			if err != nil {
				return nil, err
			}
			messages = append(messages, msgs...)
		}
		if len(messages) == 0 {
			return nil, errors.New("input did not contain any message content")
		}
		return messages, nil
	case nil:
		return nil, errors.New("input is required")
	default:
		return nil, errors.New("unsupported input format")
	}
}

func itemToMessages(item interface{}) ([]chatMessage, error) {
	switch value := item.(type) {
	case string:
		text := strings.TrimSpace(value)
		if text == "" {
			return nil, nil
		}
		return []chatMessage{{Role: "user", Content: text}}, nil
	case map[string]interface{}:
		role := strings.TrimSpace(stringFromMap(value, "role"))
		if role == "" {
			role = "user"
		}
		if role != "system" && role != "user" && role != "assistant" {
			role = "user"
		}

		if inputType := strings.TrimSpace(stringFromMap(value, "type")); inputType == "message" {
			content := flattenContent(value["content"])
			if content == "" {
				return nil, nil
			}
			return []chatMessage{{Role: role, Content: content}}, nil
		}

		content := flattenContent(value["content"])
		if content == "" {
			content = flattenContent(value["input_text"])
		}
		if content == "" {
			content = flattenContent(value["text"])
		}
		if content == "" {
			return nil, nil
		}
		return []chatMessage{{Role: role, Content: content}}, nil
	default:
		return nil, errors.New("unsupported input item")
	}
}

func flattenContent(value interface{}) string {
	switch content := value.(type) {
	case string:
		return strings.TrimSpace(content)
	case []interface{}:
		var parts []string
		for _, item := range content {
			part := flattenContent(item)
			if part != "" {
				parts = append(parts, part)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]interface{}:
		if text := strings.TrimSpace(stringFromMap(content, "text")); text != "" {
			return text
		}
		if inputText := strings.TrimSpace(stringFromMap(content, "input_text")); inputText != "" {
			return inputText
		}
		if nested, ok := content["content"]; ok {
			return flattenContent(nested)
		}
		return ""
	default:
		return ""
	}
}

func stringFromMap(m map[string]interface{}, key string) string {
	value, ok := m[key]
	if !ok {
		return ""
	}
	s, _ := value.(string)
	return s
}

func extractBearer(header string) (string, bool) {
	parts := strings.SplitN(strings.TrimSpace(header), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	token := strings.TrimSpace(parts[1])
	return token, token != ""
}

func splitModels(value string) []string {
	parts := strings.Split(value, ",")
	models := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			models = append(models, trimmed)
		}
	}
	return models
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func randomHex(bytesCount int) string {
	buf := make([]byte, bytesCount)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}

func envOrDefault(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func intEnvOrDefault(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, recorder.status, time.Since(start).Round(time.Millisecond))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

type flushWriter struct {
	http.ResponseWriter
}

func (f flushWriter) Write(p []byte) (int, error) {
	n, err := f.ResponseWriter.Write(p)
	if flusher, ok := f.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
	return n, err
}

func copyHeaders(dst http.Header, src http.Header) {
	for key, values := range src {
		if strings.EqualFold(key, "Content-Length") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeOpenAIError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]interface{}{
		"error": map[string]interface{}{
			"message": message,
			"type":    "invalid_request_error",
		},
	})
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"error": message,
	})
}

func methodNotAllowed(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
}
