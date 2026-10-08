package main

import (
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
)

// Reações recebidas. 1:1 vem como ReactionMessage (texto = emoji, vazio = reação
// removida). Grupos/canais vêm como EncReactionMessage (criptografada) —
// DecryptReaction (público no whatsmeow) resolve. Encaminhamos via webhook
// `reaction` com o ID da mensagem reagida (que é o source_id no Chatwoot), para
// o Chatwoot atrelar a reação à mensagem certa.

func (s *Session) handleIncomingReaction(evt *events.Message) {
	var rm *waE2E.ReactionMessage
	if r := evt.Message.GetReactionMessage(); r != nil {
		rm = r
	} else if evt.Message.GetEncReactionMessage() != nil {
		dec, err := s.client.DecryptReaction(s.mgr.appCtx, evt)
		if err != nil {
			s.log.Warn("decrypt reaction falhou", "err", err)
			return
		}
		rm = dec
	}
	if rm == nil {
		return
	}

	emoji := rm.GetText() // "" = reação removida
	who := evt.Info.PushName
	if who == "" {
		who = s.realPhone(evt.Info.Sender)
	}

	s.dispatchWebhook("reaction", map[string]any{
		"messageId":    rm.GetKey().GetID(), // a msg do WhatsApp reagida (= source_id no Chatwoot)
		"emoji":        emoji,
		"removed":      emoji == "",
		"reactor":      evt.Info.Sender.String(),
		"reactorPhone": s.realPhone(evt.Info.Sender),
		"reactorName":  who,
		"chat":         evt.Info.Chat.String(),
		"isGroup":      evt.Info.IsGroup,
		"fromMe":       evt.Info.IsFromMe, // true = NÓS reagimos (outro device), não o cliente
		"toMe":         rm.GetKey().GetFromMe(), // true = reagiram a uma mensagem NOSSA
		"timestamp":    evt.Info.Timestamp.UnixMilli(),
	})
}

// Revoke ("apagar para todos"). Chega como ProtocolMessage tipo REVOKE — key.ID
// é a mensagem original apagada, key.FromMe indica de quem ERA essa mensagem
// (false = o contato apagou uma mensagem que ELE mandou; true = apagamos a
// nossa de outro device). Webhook `revoke` dedicado — não é mensagem nova.
func (s *Session) handleIncomingRevoke(evt *events.Message) {
	pm := evt.Message.GetProtocolMessage()
	key := pm.GetKey()
	if key.GetID() == "" {
		return
	}
	s.dispatchWebhook("revoke", map[string]any{
		"messageId":    key.GetID(),
		"chat":         evt.Info.Chat.String(),
		"isGroup":      evt.Info.IsGroup,
		"fromMe":       key.GetFromMe(),
		"participant":  key.GetParticipant(),
		"revokerPhone": s.realPhone(evt.Info.Sender),
		"timestamp":    evt.Info.Timestamp.UnixMilli(),
	})
}

// Edição ("editar mensagem"). Chega como ProtocolMessage tipo MESSAGE_EDIT —
// key.ID é a mensagem original, EditedMessage traz o conteúdo novo. key.FromMe
// indica de quem ERA a mensagem original (false = o contato editou uma
// mensagem que ELE mandou).
func (s *Session) handleIncomingEdit(evt *events.Message) {
	pm := evt.Message.GetProtocolMessage()
	key := pm.GetKey()
	newText := messageText(pm.GetEditedMessage())
	if key.GetID() == "" || newText == "" {
		return
	}
	s.dispatchWebhook("edit", map[string]any{
		"messageId":   key.GetID(),
		"text":        newText,
		"chat":        evt.Info.Chat.String(),
		"isGroup":     evt.Info.IsGroup,
		"fromMe":      key.GetFromMe(),
		"participant": key.GetParticipant(),
		"editorPhone": s.realPhone(evt.Info.Sender),
		"timestamp":   evt.Info.Timestamp.UnixMilli(),
	})
}

// Mesma ideia de handleIncomingEdit, pro formato alternativo (stanza edit="1",
// sem ProtocolMessage — ver comentário no case em session.go). Aqui não há
// "key" próprio: o id do stanza (evt.Info.ID) É o da mensagem original.
func (s *Session) handleIncomingEditAlt(evt *events.Message) {
	newText := messageText(evt.Message.GetEditedMessage().GetMessage())
	if evt.Info.ID == "" || newText == "" {
		return
	}
	s.dispatchWebhook("edit", map[string]any{
		"messageId":   string(evt.Info.ID),
		"text":        newText,
		"chat":        evt.Info.Chat.String(),
		"isGroup":     evt.Info.IsGroup,
		"fromMe":      evt.Info.IsFromMe,
		"editorPhone": s.realPhone(evt.Info.Sender),
		"timestamp":   evt.Info.Timestamp.UnixMilli(),
	})
}

// Terceiro formato de edição — o mais comum em clientes recentes do WhatsApp:
// chega como SecretEncryptedMessage (tipo MESSAGE_EDIT), igual EncReactionMessage
// e voto de enquete. targetMessageKey aponta a mensagem original; o conteúdo novo
// só decifra com o messageSecret dela, que o whatsmeow já guardou sozinho em
// Store.MsgSecrets quando a mensagem original chegou (storeMessageSecret, em
// processProtocolParts — roda pra toda mensagem recebida, não só protocol).
// DecryptSecretEncryptedMessage (público no whatsmeow) resolve a partir daí.
func (s *Session) handleIncomingEditSecret(evt *events.Message) {
	sec := evt.Message.GetSecretEncryptedMessage()
	targetKey := sec.GetTargetMessageKey()
	if targetKey.GetID() == "" {
		return
	}
	decoded, err := s.client.DecryptSecretEncryptedMessage(s.mgr.appCtx, evt)
	if err != nil {
		s.log.Warn("decrypt edit (secretEncrypted) falhou", "err", err, "messageId", targetKey.GetID())
		return
	}
	newText := messageText(decoded)
	if newText == "" {
		return
	}
	s.dispatchWebhook("edit", map[string]any{
		"messageId":   targetKey.GetID(),
		"text":        newText,
		"chat":        evt.Info.Chat.String(),
		"isGroup":     evt.Info.IsGroup,
		"fromMe":      targetKey.GetFromMe(),
		"participant": targetKey.GetParticipant(),
		"editorPhone": s.realPhone(evt.Info.Sender),
		"timestamp":   evt.Info.Timestamp.UnixMilli(),
	})
}
