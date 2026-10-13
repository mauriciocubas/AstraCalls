package call

import (
	"fmt"
	"time"
	"wacalls/internal/voip/core"
)

type CallStateData struct {
	State        core.CallState
	ConnectedAt  *time.Time
	AcceptedAt   *time.Time
	EndedAt      *time.Time
	AudioMuted   bool
	VideoOff     bool
	Silenced     bool
	EndReason    core.EndCallReason
	DurationSecs int

	// Negociação de vídeo mid-call (só relevante com a call ativa).
	PeerVideoOn          bool // câmera do peer está ligada
	VideoUpgradeIncoming bool // o peer pediu upgrade e aguarda nossa decisão
	VideoUpgradeOutgoing bool // nós pedimos upgrade e aguardamos o peer
}

type CallInfo struct {
	CallID          string
	PeerJid         string
	CallCreator     string
	Direction       core.CallDirection
	MediaType       core.CallMediaType
	StateData       CallStateData
	CreatedAt       time.Time
	GroupJid        string
	IsOffline       bool
	CallerPn        string
	EncryptionKey   []byte
	RelayData       *core.RelayData
	ElectedRelayIdx *int
}

func NewOutgoingCall(callID, peerJid, ourJid string, mediaType core.CallMediaType) *CallInfo {
	return &CallInfo{
		CallID:      callID,
		PeerJid:     peerJid,
		CallCreator: ourJid,
		Direction:   core.CallDirectionOutgoing,
		MediaType:   mediaType,
		CreatedAt:   time.Now(),
		StateData: CallStateData{
			State:      core.CallStateInitiating,
			AudioMuted: false,
			VideoOff:   mediaType != core.CallMediaTypeVideo,
		},
	}
}

func NewIncomingCall(callID, peerJid, callCreator, callerPn string, mediaType core.CallMediaType) *CallInfo {
	return &CallInfo{
		CallID:      callID,
		PeerJid:     peerJid,
		CallCreator: callCreator,
		Direction:   core.CallDirectionIncoming,
		MediaType:   mediaType,
		CreatedAt:   time.Now(),
		CallerPn:    callerPn,
		StateData: CallStateData{
			State:      core.CallStateIncomingRinging,
			AudioMuted: false,
			VideoOff:   mediaType != core.CallMediaTypeVideo,
		},
	}
}

func (c *CallInfo) IsInitiator() bool { return c.Direction == core.CallDirectionOutgoing }

func (c *CallInfo) IsActive() bool { return c.StateData.State == core.CallStateActive }

func (c *CallInfo) IsRinging() bool {
	return c.StateData.State == core.CallStateRinging || c.StateData.State == core.CallStateIncomingRinging
}

func (c *CallInfo) IsEnded() bool { return c.StateData.State == core.CallStateEnded }

// HasMediaAccepted indica que a chamada já saiu da disputa de toque entre
// dispositivos (ringing/incoming_ringing) — só a partir daqui o peer e as
// chaves SRTP estão definitivos. Usado para não alimentar o decoder Opus com
// pacotes de áudio que cheguem antes do accept (ex.: enquanto vários devices
// do destino ainda estão tocando), que é a janela onde vimos o crash do
// resampler do SILK (libopus_mlow) em produção.
func (c *CallInfo) HasMediaAccepted() bool {
	switch c.StateData.State {
	case core.CallStateConnecting, core.CallStateActive, core.CallStateOnHold:
		return true
	default:
		return false
	}
}

func (c *CallInfo) CanAccept() bool { return c.StateData.State == core.CallStateIncomingRinging }

func (c *CallInfo) CanReject() bool {
	return c.StateData.State == core.CallStateIncomingRinging || c.StateData.State == core.CallStateRinging
}

type InvalidTransition struct {
	CurrentState string
	Attempted    string
}

func (e *InvalidTransition) Error() string {
	return fmt.Sprintf("invalid transition '%s' in state '%s'", e.Attempted, e.CurrentState)
}

const (
	TransitionOfferSent         = "offer_sent"
	TransitionOfferReceived     = "offer_received"
	TransitionLocalAccepted     = "local_accepted"
	TransitionRemoteAccepted    = "remote_accepted"
	TransitionLocalRejected     = "local_rejected"
	TransitionRemoteRejected    = "remote_rejected"
	TransitionMediaConnected    = "media_connected"
	TransitionTerminated        = "terminated"
	TransitionHold              = "hold"
	TransitionResume            = "resume"
	TransitionAudioMuteChanged  = "audio_mute_changed"
	TransitionVideoStateChanged = "video_state_changed"
)

type Transition struct {
	Type     string
	Reason   core.EndCallReason
	Muted    bool
	Off      bool
	Silenced bool
}

func (c *CallInfo) ApplyTransition(t Transition) error {
	s := &c.StateData
	now := time.Now()

	switch t.Type {
	case TransitionOfferSent:
		if s.State != core.CallStateInitiating {
			return &InvalidTransition{string(s.State), t.Type}
		}
		s.State = core.CallStateRinging

	case TransitionOfferReceived:
		if s.State != core.CallStateInitiating {
			return &InvalidTransition{string(s.State), t.Type}
		}
		s.State = core.CallStateIncomingRinging
		s.Silenced = t.Silenced

	case TransitionRemoteAccepted:
		if s.State != core.CallStateRinging {
			return &InvalidTransition{string(s.State), t.Type}
		}
		s.State = core.CallStateConnecting
		s.AcceptedAt = &now

	case TransitionLocalAccepted:
		if s.State != core.CallStateIncomingRinging {
			return &InvalidTransition{string(s.State), t.Type}
		}
		s.State = core.CallStateConnecting
		s.AcceptedAt = &now

	case TransitionRemoteRejected:
		if s.State != core.CallStateRinging {
			return &InvalidTransition{string(s.State), t.Type}
		}
		s.State = core.CallStateEnded
		s.EndedAt = &now
		s.EndReason = t.Reason

	case TransitionLocalRejected:
		if s.State != core.CallStateIncomingRinging {
			return &InvalidTransition{string(s.State), t.Type}
		}
		s.State = core.CallStateEnded
		s.EndedAt = &now
		s.EndReason = t.Reason

	case TransitionMediaConnected:
		if s.State != core.CallStateConnecting {
			return &InvalidTransition{string(s.State), t.Type}
		}
		s.State = core.CallStateActive
		s.ConnectedAt = &now
		s.VideoOff = c.MediaType != core.CallMediaTypeVideo

	case TransitionTerminated:
		if s.State == core.CallStateEnded {
			return &InvalidTransition{string(s.State), t.Type}
		}
		if (s.State == core.CallStateActive || s.State == core.CallStateOnHold) && s.ConnectedAt != nil {
			s.DurationSecs = int(now.Sub(*s.ConnectedAt).Seconds())
		}
		s.State = core.CallStateEnded
		s.EndedAt = &now
		s.EndReason = t.Reason

	case TransitionHold:
		if s.State != core.CallStateActive {
			return &InvalidTransition{string(s.State), t.Type}
		}
		s.State = core.CallStateOnHold

	case TransitionResume:
		if s.State != core.CallStateOnHold {
			return &InvalidTransition{string(s.State), t.Type}
		}
		s.State = core.CallStateActive

	case TransitionAudioMuteChanged:
		if s.State != core.CallStateActive {
			return &InvalidTransition{string(s.State), t.Type}
		}
		s.AudioMuted = t.Muted

	case TransitionVideoStateChanged:
		if s.State != core.CallStateActive {
			return &InvalidTransition{string(s.State), t.Type}
		}
		s.VideoOff = t.Off

	default:
		return &InvalidTransition{string(s.State), t.Type}
	}

	return nil
}
