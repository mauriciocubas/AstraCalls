# Patch local: streaming sidecar também pra áudio/PTT

Cópia de `go.mau.fi/whatsmeow` na mesma revisão pinada no `go.mod`
(`v0.0.0-20261102152715-54038813c47f`), com um patch de 2 pontos em
`upload.go`: `Upload()` e `UploadReader()` só geravam `StreamingSidecar`
pra `MediaVideo` (ver PR upstream tulir/whatsmeow#1303 — "Messenger's
iOS and Android apps show 'Couldn't download' for encrypted videos
that don't have a streaming sidecar"). O WhatsApp passou a exigir o
mesmo sidecar pra áudio/PTT; sem ele a mídia chega no destino mas o
botão de baixar não funciona e o arquivo nunca abre.

Mudança: estende a condição pra `appInfo == MediaVideo || appInfo == MediaAudio`
nos dois pontos. `cmd/server/messaging.go` também precisou passar
`StreamingSidecar: up.StreamingSidecar` pro `AudioMessage` e pro
`VideoMessage` (o campo já vinha no `UploadResponse`, mas nunca era
copiado pra mensagem).

Referenciado via `replace` em `go.mod`:

	replace go.mau.fi/whatsmeow => ./third_party/whatsmeow

Quando o upstream corrigir isso pra áudio (acompanhar
github.com/tulir/whatsmeow), remover este diretório e o `replace`,
voltar a usar a versão normal do módulo.
