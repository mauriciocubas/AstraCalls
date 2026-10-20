package wa

import (
	"context"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/signaling"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

type Socket struct {
	cli *whatsmeow.Client
}

func NewSocket(cli *whatsmeow.Client) *Socket { return &Socket{cli: cli} }

var _ core.VoipSocket = (*Socket)(nil)

func (s *Socket) di() *whatsmeow.DangerousInternalClient { return s.cli.DangerousInternals() }

func (s *Socket) OwnPN() types.JID { return s.di().GetOwnID() }

func (s *Socket) OwnLID() types.JID { return s.di().GetOwnLID() }

func (s *Socket) AccountDeviceIdentityNode() (waBinary.Node, bool) {
	if s.cli.Store == nil || s.cli.Store.Account == nil {
		return waBinary.Node{}, false
	}
	return s.di().MakeDeviceIdentityNode(), true
}

func (s *Socket) SendNode(ctx context.Context, node waBinary.Node) error {
	return s.di().SendNode(ctx, node)
}

func (s *Socket) Query(ctx context.Context, node waBinary.Node) (*waBinary.Node, error) {
	id, _ := node.Attrs["id"].(string)
	if id == "" {
		return nil, s.di().SendNode(ctx, node)
	}
	di := s.di()
	ch := di.WaitResponse(id)
	if err := di.SendNode(ctx, node); err != nil {
		di.CancelResponse(id, ch)
		return nil, err
	}
	select {
	case resp := <-ch:
		return resp, nil
	case <-time.After(15 * time.Second):
		di.CancelResponse(id, ch)
		return nil, nil
	case <-ctx.Done():
		di.CancelResponse(id, ch)
		return nil, ctx.Err()
	}
}

func (s *Socket) GetUSyncDevices(ctx context.Context, jids []types.JID) ([]types.JID, error) {
	return s.cli.GetUserDevices(ctx, jids)
}

// AssertSessions prepara o store Signal antes de cifrar a call key do offer.
// Antes era stub (return nil): sessões PN+LID duplicadas/corrompidas geravam
// terminate reason=enc no peer enquanto a linha Principal (só LID) ligava.
//
// - PN conhecido → migra store pra LID e apaga restos PN
// - LID → apaga sessões PN espelho (mesmo contato) se o mapa LID↔PN existir
// - force → descarta sessão/identity do peer e busca prekeys de novo
// - sem sessão → FetchPreKeys pra o EncryptMessageForDevices fazer pkmsg limpo
func (s *Socket) AssertSessions(ctx context.Context, jids []types.JID, force bool) error {
	if s.cli == nil || s.cli.Store == nil || s.cli.Store.Sessions == nil {
		return nil
	}
	di := s.di()
	var missing []types.JID
	seenUser := map[string]bool{}

	for _, jid := range jids {
		enc := jid
		if jid.Server == types.DefaultUserServer {
			if lid := s.ResolveLIDForPN(ctx, jid); !lid.IsEmpty() && lid.Server == types.HiddenUserServer {
				di.MigrateSessionStore(ctx, jid, lid)
				// DeleteAllSessions usa prefixo SignalAddressUser (PN cru / LID com _1).
				s.dropSignalUser(ctx, jid.SignalAddressUser())
				enc = types.JID{User: lid.User, Device: jid.Device, Server: types.HiddenUserServer}
			}
		} else if jid.Server == types.HiddenUserServer && s.cli.Store.LIDs != nil {
			if pn, err := s.cli.Store.LIDs.GetPNForLID(ctx, types.JID{User: jid.User, Server: types.HiddenUserServer}); err == nil && !pn.IsEmpty() {
				di.MigrateSessionStore(ctx, pn, jid)
				s.dropSignalUser(ctx, pn.SignalAddressUser())
			}
		}

		sigUser := enc.SignalAddressUser()
		if force && !seenUser[sigUser] {
			seenUser[sigUser] = true
			s.dropSignalUser(ctx, sigUser)
		}

		addr := enc.SignalAddress().String()
		has, err := s.cli.Store.Sessions.HasSession(ctx, addr)
		if err != nil {
			return err
		}
		if !has || force {
			missing = append(missing, enc)
		}
	}

	if len(missing) == 0 {
		return nil
	}
	_ = di.FetchPreKeysNoError(ctx, missing)
	return nil
}

func (s *Socket) dropSignalUser(ctx context.Context, user string) {
	if user == "" || s.cli.Store == nil {
		return
	}
	if s.cli.Store.Sessions != nil {
		_ = s.cli.Store.Sessions.DeleteAllSessions(ctx, user)
	}
	if s.cli.Store.Identities != nil {
		_ = s.cli.Store.Identities.DeleteAllIdentities(ctx, user)
	}
}

func (s *Socket) CreateParticipantNodes(ctx context.Context, devices []types.JID, callKey []byte, encAttrs waBinary.Attrs) ([]waBinary.Node, bool, error) {
	plaintext, err := signaling.EncodeCallKeyMessage(callKey)
	if err != nil {
		return nil, false, err
	}
	id := s.cli.GenerateMessageID()
	return s.di().EncryptMessageForDevices(ctx, devices, id, plaintext, plaintext, encAttrs)
}

func (s *Socket) DecryptCallKey(ctx context.Context, from types.JID, encChild *waBinary.Node) ([]byte, error) {
	typ, _ := encChild.Attrs["type"].(string)
	isPreKey := typ == "pkmsg"
	plaintext, _, err := s.di().DecryptDM(ctx, encChild, from, isPreKey, time.Now())
	if err != nil {
		return nil, err
	}
	return signaling.DecodeCallKeyPlaintext(plaintext)
}

// GetTCToken: stub (igual ao original que toca). Comprovado no log bruto: anexar
// o tctoken guardado (stale) faz o destino DESCARTAR o offer em silêncio — até
// números que aceitariam param de tocar. Sem token, o offer toca (preaccept).
// O 463 em números com privacidade estrita é anti-spam do WhatsApp (sem fix).
func (s *Socket) GetTCToken(ctx context.Context, jid types.JID) ([]byte, error) {
	return nil, nil
}

func (s *Socket) ResolveLIDForPN(ctx context.Context, pn types.JID) types.JID {
	if pn.Server == types.HiddenUserServer {
		return pn
	}
	if s.cli.Store != nil && s.cli.Store.LIDs != nil {
		if lid, err := s.cli.Store.LIDs.GetLIDForPN(ctx, pn); err == nil && !lid.IsEmpty() {
			return lid
		}
	}
	return pn
}
