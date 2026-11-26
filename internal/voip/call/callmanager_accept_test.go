package call

import (
	"context"
	"log/slog"
	"testing"

	"wacalls/internal/voip/core"
)

// Achado ao vivo 26/11 (Alpha Piscinas): AcceptCall retornava nil (sucesso)
// mesmo sem a call key — nunca mandava o accept pro WhatsApp, e ninguém
// descobria. Regressão: tem que falhar explicitamente, sem mandar nada.
func TestAcceptCallSemKeyFalhaExplicito(t *testing.T) {
	sock := &fakeSock{}
	m := NewCallManager(sock, slog.Default())
	call := NewIncomingCall("CALLID", "peer@lid", "creator@lid", "", core.CallMediaTypeAudio)
	// EncryptionKey fica nil de propósito — simula offer que não decifrou.
	m.currentCall = call

	if err := m.AcceptCall(context.Background(), "CALLID"); err == nil {
		t.Fatal("esperava erro quando falta a call key — AcceptCall não pode voltar sucesso sem avisar o WhatsApp")
	}
	if len(sock.sent) != 0 {
		t.Fatalf("sem key não devia mandar nenhuma stanza, mandou %d", len(sock.sent))
	}
}

// Com key presente, AcceptCall tem que seguir em frente e tentar montar o
// accept de verdade — não pode cair no mesmo erro de "falta key".
func TestAcceptCallComKeyNaoCaiNoErroDeKeyAusente(t *testing.T) {
	sock := &fakeSock{}
	m := NewCallManager(sock, slog.Default())
	call := NewIncomingCall("CALLID", "peer@lid", "creator@lid", "", core.CallMediaTypeAudio)
	call.EncryptionKey = make([]byte, 32)
	m.currentCall = call

	err := m.AcceptCall(context.Background(), "CALLID")
	// fakeSock.CreateParticipantNodes não produz nó "enc" — BuildAcceptStanza
	// falha por esse motivo (ambiente de teste, não por falta de key). O que
	// importa aqui é que não seja o erro "missing call encryption key".
	if err == nil {
		t.Fatal("fakeSock não gera enc node — esperava erro de build do accept")
	}
	if ce, ok := err.(*CallError); ok && ce.Msg == "missing call encryption key — offer decrypt failed, cannot signal accept to peer" {
		t.Fatal("não devia cair no erro de key ausente quando a key está presente")
	}
}
