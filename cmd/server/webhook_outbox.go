package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Fila de reentrega durável do webhook da sessão → integrador (Foorce Zap).
//
// Motivação: dispatchWebhook era fire-and-forget. Se o Foorce está fora
// (OOM, restart, deploy) no instante do POST, a mensagem some — achado ao
// vivo 2026-11-09 (Nop Agência / Abinsaut: 11:38–11:44 sumiram no Canal
// Aberto enquanto o whatsup reiniciava).
//
// Mesmo desenho do chatwoot_outbox: persiste no banco PRINCIPAL, worker com
// backoff, UNIQUE (session_id, source_id) pra não duplicar. O Foorce já
// deduplica por whatsapp_message_id — reentrega é segura.
//
// Não muda o formato do POST ({session, event, timestamp, data}).

const (
	whRetryBase   = 15 * time.Second
	whRetryMax    = 10 * time.Minute
	whMaxAttempts = 24 // ~15s…10min × 24 ≈ várias horas de cobertura pós-deploy
	whWorkerTick  = 10 * time.Second
	whBatchSize   = 80
	// Payload com mídia base64 pode ser grande; acima disso enfileira sem
	// "media" (texto/metadados ainda chegam) pra não estourar o banco.
	whMaxPayloadBytes = 1_500_000
)

func webhookBackoff(attempts int) time.Duration {
	d := whRetryBase
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= whRetryMax {
			return whRetryMax
		}
	}
	return d
}

type webhookOutboxRow struct {
	ID        int64
	SessionID string
	URL       string
	Payload   []byte
	Attempts  int
}

// webhookSourceID deriva a chave de idempotência a partir do evento + data.
// Preferimos IDs estáveis do WhatsApp; fallback = hash do payload.
func webhookSourceID(event string, data any, body []byte) string {
	event = strings.TrimSpace(event)
	if event == "" {
		event = "unknown"
	}
	m, _ := data.(map[string]any)
	if m != nil {
		for _, key := range []string{"id", "messageId"} {
			if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
				return event + ":" + strings.TrimSpace(v)
			}
		}
		// receipt: vários ids — junta de forma estável
		if ids, ok := m["ids"].([]any); ok && len(ids) > 0 {
			parts := make([]string, 0, len(ids))
			for _, raw := range ids {
				parts = append(parts, fmt.Sprint(raw))
			}
			typ, _ := m["type"].(string)
			return fmt.Sprintf("%s:%s:%s", event, typ, strings.Join(parts, ","))
		}
		if ids, ok := m["ids"].([]string); ok && len(ids) > 0 {
			typ, _ := m["type"].(string)
			return fmt.Sprintf("%s:%s:%s", event, typ, strings.Join(ids, ","))
		}
	}
	sum := sha256.Sum256(body)
	return event + ":h:" + hex.EncodeToString(sum[:12])
}

func (s *sessionStore) enqueueWebhookOutbox(ctx context.Context, sessionID, sourceID, url string, payload []byte, nextAt, createdAt int64) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO webhook_outbox (session_id, source_id, url, payload, attempts, next_at, created_at, dead)
		VALUES ($1, $2, $3, $4, 0, $5, $6, false)
		ON CONFLICT (session_id, source_id) DO UPDATE
			SET payload = EXCLUDED.payload, url = EXCLUDED.url, next_at = EXCLUDED.next_at,
			    attempts = 0, dead = false, last_error = NULL`,
		sessionID, sourceID, url, payload, nextAt, createdAt)
	return err
}

func (s *sessionStore) dueWebhookOutbox(ctx context.Context, now int64, limit int) ([]webhookOutboxRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, session_id, url, payload, attempts
		FROM webhook_outbox
		WHERE dead = false AND next_at <= $1
		ORDER BY next_at
		LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []webhookOutboxRow{}
	for rows.Next() {
		var r webhookOutboxRow
		if err := rows.Scan(&r.ID, &r.SessionID, &r.URL, &r.Payload, &r.Attempts); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sessionStore) rescheduleWebhookOutbox(ctx context.Context, id int64, attempts int, nextAt int64, lastErr string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE webhook_outbox SET attempts = $1, next_at = $2, last_error = $3 WHERE id = $4`,
		attempts, nextAt, lastErr, id)
	return err
}

func (s *sessionStore) killWebhookOutbox(ctx context.Context, id int64, lastErr string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE webhook_outbox SET dead = true, last_error = $1 WHERE id = $2`, lastErr, id)
	return err
}

func (s *sessionStore) deleteWebhookOutbox(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM webhook_outbox WHERE id = $1`, id)
	return err
}

func (s *sessionStore) deleteWebhookOutboxForSession(ctx context.Context, sessionID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM webhook_outbox WHERE session_id = $1`, sessionID)
	return err
}

// purgeOldWebhookOutbox remove dead-letters antigas e lixo órfão (janela 7d).
func (s *sessionStore) purgeOldWebhookOutbox(ctx context.Context, olderThanMs int64) {
	_, _ = s.db.ExecContext(ctx,
		`DELETE FROM webhook_outbox WHERE dead = true AND created_at < $1`, olderThanMs)
}

func (m *SessionManager) runWebhookOutbox(ctx context.Context) {
	t := time.NewTicker(whWorkerTick)
	defer t.Stop()
	purgeEvery := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.drainWebhookOutbox(ctx)
			purgeEvery++
			if purgeEvery >= 60 { // ~10min
				purgeEvery = 0
				if m.store != nil {
					m.store.purgeOldWebhookOutbox(ctx, nowMillis()-7*24*time.Hour.Milliseconds())
				}
			}
		}
	}
}

func (m *SessionManager) drainWebhookOutbox(ctx context.Context) {
	if m.store == nil {
		return
	}
	rows, err := m.store.dueWebhookOutbox(ctx, nowMillis(), whBatchSize)
	if err != nil {
		m.log.Error("webhook outbox: consultar pendências falhou", "err", err)
		return
	}
	for _, row := range rows {
		m.processWebhookOutboxRow(ctx, row)
	}
}

func (m *SessionManager) processWebhookOutboxRow(ctx context.Context, row webhookOutboxRow) {
	url := strings.TrimSpace(row.URL)
	if url == "" {
		// URL sumiu (webhook desregistrado): não martela — dead-letter.
		_ = m.store.killWebhookOutbox(ctx, row.ID, "url vazia")
		return
	}
	// Se a sessão ainda existe, prefere a URL atual (pode ter sido atualizada
	// com secret novo). Senão usa a URL gravada na pendência.
	if sess, ok := m.Get(row.SessionID); ok {
		if live := strings.TrimSpace(sess.getWebhook()); live != "" {
			url = live
		}
	}
	err := postWebhookOnce(ctx, url, row.Payload)
	if err != nil {
		attempts := row.Attempts + 1
		if attempts >= whMaxAttempts {
			m.log.Error("webhook outbox: dead-letter após esgotar tentativas",
				"id", row.ID, "session", row.SessionID, "attempts", attempts, "err", err)
			_ = m.store.killWebhookOutbox(ctx, row.ID, err.Error())
			return
		}
		delay := webhookBackoff(attempts)
		m.log.Warn("webhook outbox: reentrega falhou, reagendando",
			"id", row.ID, "session", row.SessionID, "attempt", attempts, "delay", delay.String(), "err", err)
		_ = m.store.rescheduleWebhookOutbox(ctx, row.ID, attempts,
			nowMillis()+delay.Milliseconds(), err.Error())
		return
	}
	_ = m.store.deleteWebhookOutbox(ctx, row.ID)
	m.log.Info("webhook outbox: reentrega concluída", "id", row.ID, "session", row.SessionID)
}

func postWebhookOnce(ctx context.Context, url string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := webhookClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	// 4xx (exceto 408/429) = configuração/secret — não adianta martelar igual
	// 5xx; ainda assim reagenda algumas vezes (deploy parcial) e depois dead.
	return fmt.Errorf("http %d", resp.StatusCode)
}

// enqueueWebhookOutbox grava pendência após falha do POST imediato.
func (s *Session) enqueueWebhookOutbox(event string, data any, url string, body []byte) {
	if s.mgr == nil || s.mgr.store == nil || len(body) == 0 || url == "" {
		return
	}
	payload := body
	if len(payload) > whMaxPayloadBytes {
		// Reconstrói envelope sem mídia/raw (continua JSON válido). Sem truncar
		// bytes crus — JSONB rejeitaria e o outbox falharia em silêncio.
		var envelope map[string]any
		if err := json.Unmarshal(body, &envelope); err == nil {
			if dataMap, ok := data.(map[string]any); ok {
				trimmed := make(map[string]any, len(dataMap))
				for k, v := range dataMap {
					if k == "media" || k == "raw" {
						continue
					}
					trimmed[k] = v
				}
				trimmed["mediaOmitted"] = true
				envelope["data"] = trimmed
				if b, err := json.Marshal(envelope); err == nil {
					payload = b
				}
			}
		}
		if len(payload) > whMaxPayloadBytes {
			s.log.Warn("webhook outbox: payload ainda grande após omitir mídia — enfileira mesmo assim",
				"bytes", len(payload))
		}
	}
	sourceID := webhookSourceID(event, data, payload)
	now := nowMillis()
	if err := s.mgr.store.enqueueWebhookOutbox(s.mgr.appCtx, s.id, sourceID, url, payload,
		now+whRetryBase.Milliseconds(), now); err != nil {
		s.log.Error("webhook outbox: enfileirar falhou", "err", err, "source", sourceID)
		return
	}
	s.log.Warn("webhook outbox: POST falhou — pendência gravada", "event", event, "source", sourceID)
}
