package main

import (
	"strings"
	"testing"
)

func TestWebhookSourceID_messageID(t *testing.T) {
	got := webhookSourceID("message", map[string]any{"id": "3EB0ABC"}, nil)
	if got != "message:3EB0ABC" {
		t.Fatalf("got %q", got)
	}
}

func TestWebhookSourceID_messageIdAlt(t *testing.T) {
	got := webhookSourceID("edit", map[string]any{"messageId": "XYZ"}, nil)
	if got != "edit:XYZ" {
		t.Fatalf("got %q", got)
	}
}

func TestWebhookSourceID_receiptIds(t *testing.T) {
	got := webhookSourceID("receipt", map[string]any{
		"type": "read",
		"ids":  []any{"A", "B"},
	}, nil)
	if got != "receipt:read:A,B" {
		t.Fatalf("got %q", got)
	}
}

func TestWebhookSourceID_fallbackHash(t *testing.T) {
	body := []byte(`{"session":"s1","event":"broadcast"}`)
	got := webhookSourceID("broadcast", map[string]any{"campaignId": 1}, body)
	if !strings.HasPrefix(got, "broadcast:h:") {
		t.Fatalf("expected hash fallback, got %q", got)
	}
}

func TestWebhookBackoff(t *testing.T) {
	if webhookBackoff(1) != whRetryBase {
		t.Fatalf("attempt 1")
	}
	if webhookBackoff(2) != 2*whRetryBase {
		t.Fatalf("attempt 2")
	}
	if webhookBackoff(99) != whRetryMax {
		t.Fatalf("capped")
	}
}
