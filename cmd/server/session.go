package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"wacalls/internal/voip/call"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/signaling"
	"wacalls/internal/voip/wanode"
	"wacalls/internal/wa"

	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type Session struct {
	id   string
	name string
	mgr  *SessionManager
	log  *slog.Logger

	client *whatsmeow.Client
	reg    *callRegistry

	mu   sync.Mutex
	auth AuthSnapshot

	presenceOnce sync.Once // garante 1 keepalive de presença por sessão
}

func newSession(mgr *SessionManager, id, name string, client *whatsmeow.Client) *Session {
	s := &Session{
		id:     id,
		name:   name,
		mgr:    mgr,
		log:    mgr.log.With("session", id),
		client: client,
		auth:   AuthSnapshot{State: "connecting"},
		reg:    newCallRegistry(),
	}
	client.AddEventHandler(s.handleEvent)
	return s
}

func (s *Session) createCall(callID string) *call.CallManager {
	cm := call.NewCallManager(wa.NewSocket(s.client), s.log)
	s.wireCall(cm, callID)
	s.reg.add(callID, &activeCall{cm: cm})
	return cm
}

func (s *Session) wireCall(cm *call.CallManager, callID string) {
	cm.OnIncoming = func(c *call.CallInfo) {
		s.mgr.broker.upsertCall(CallRecord{
			SessionID: s.id, CallID: c.CallID, Direction: "inbound", Peer: c.PeerJid,
			StartedAt: time.Now().UnixMilli(), Status: StatusRinging,
		})
		s.mgr.broker.emitIncoming(s.id, c.CallID, c.PeerJid, c.MediaType == core.CallMediaTypeVideo)
	}
	cm.OnStateChange = func(c *call.CallInfo) {
		if c.IsEnded() {
			s.removeCall(c.CallID)
			s.mgr.broker.endCall(c.CallID, string(c.StateData.EndReason))
			return
		}
		dir := "outbound"
		if c.Direction == core.CallDirectionIncoming {
			dir = "inbound"
		}
		existing, _ := s.mgr.broker.getCall(c.CallID)
		rec := CallRecord{
			SessionID: s.id, CallID: c.CallID, Direction: dir, Peer: c.PeerJid,
			StartedAt: time.Now().UnixMilli(), Status: mapStatus(c.StateData.State),
		}
		if existing != nil {
			rec.Owner = existing.Owner
			rec.StartedAt = existing.StartedAt
		}
		s.mgr.broker.upsertCall(rec)
	}
	cm.OnEnded = func(c *call.CallInfo) {
		s.removeCall(c.CallID)
		s.mgr.broker.endCall(c.CallID, string(c.StateData.EndReason))
	}
	cm.OnPeerAudio = func(pcm16 []float32) {
		ac, ok := s.reg.get(callID)
		if !ok || ac.bridge == nil {
			return
		}
		_ = ac.bridge.WritePCM(pcm16)
	}
	cm.OnPeerVideo = func(au []byte) {
		ac, ok := s.reg.get(callID)
		if !ok || ac.bridge == nil {
			return
		}
		_ = ac.bridge.WriteVideo(au)
	}
}

func (s *Session) startOutgoing(ctx context.Context, peer types.JID, isVideo bool) (string, error) {
	callID := signaling.GenerateCallID()
	cm := s.createCall(callID)
	if err := cm.StartCall(ctx, callID, peer, isVideo); err != nil {
		s.removeCall(callID)
		return "", err
	}
	return callID, nil
}

func (s *Session) callForEvent(from types.JID, data *waBinary.Node) (*activeCall, bool) {
	callID := callIDFromNode(wrapCall(from, data))
	if callID == "" {
		return nil, false
	}
	return s.reg.get(callID)
}

func (s *Session) onIncomingOffer(ctx context.Context, evt *events.CallOffer) {
	node := wrapCall(evt.From, evt.Data)
	callID := callIDFromNode(node)
	if callID == "" {
		return
	}
	if max := s.mgr.maxCalls; max > 0 && s.reg.count() >= max {
		s.rejectOffer(ctx, node, evt.From)
		return
	}
	cm := s.createCall(callID)
	cm.HandleCallOffer(ctx, node, evt.From)
}

func (s *Session) rejectOffer(ctx context.Context, node *waBinary.Node, from types.JID) {
	info := signaling.ExtractNodeInfo(node)
	if info == nil {
		return
	}
	creator := wanode.AttrString(info.InnerNode.Attrs, "call-creator")
	if creator == "" {
		creator = from.String()
	}
	reject := signaling.BuildRejectStanza(from, info.CallID, wanode.MustJID(creator))
	_ = wa.NewSocket(s.client).SendNode(ctx, reject)
	s.log.Info("inbound call rejected: session at capacity", "call_id", info.CallID)
}

func (s *Session) handleEvent(rawEvt any) {
	ctx := context.Background()
	switch evt := rawEvt.(type) {
	case *events.Connected:
		if id := s.client.Store.ID; id != nil {
			_ = s.mgr.store.setJID(s.mgr.appCtx, s.id, id.String())
		}
		s.setAuth(AuthSnapshot{State: "open", Paired: true})
		// Companheiro (coex) só continua "callable" se anunciar presença ativa.
		// Sem isso o WA tende a marcar o device como uncallable após a 1ª ligação
		// → a 2ª nem toca. Anuncia no connect e mantém com keepalive.
		s.markAvailable(ctx)
		s.startPresenceKeepalive()
	case *events.LoggedOut:
		s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
	case *events.CallOffer:
		s.onIncomingOffer(ctx, evt)
	case *events.CallAccept:
		s.log.Info("🔬 [CALL-DBG] accept node", "from", evt.From.String())
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			// COEX/multi-device: numa chamada ENTRANTE nós atendemos ENVIANDO
			// accept (doAccept), nunca RECEBENDO. Um accept recebido aqui vem de
			// outro device da própria conta (ex.: hosted "…:99") reivindicando a
			// chamada → transicionava pra "atendida" e sumia/não tocava o dock.
			// Ignoramos: assim o dock/IA continua podendo atender.
			if ac.cm.CurrentIsIncoming() || s.isSelfDevice(evt.From) {
				s.log.Info("🛡️ ignorando 'accept' recebido em chamada entrante (coex; mantém o dock)", "from", evt.From.String())
				return
			}
			ac.cm.HandleCallAccept(ctx, wrapCall(evt.From, evt.Data), evt.From)
		}
	case *events.CallTransport:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTransport(ctx, wrapCall(evt.From, evt.Data), evt.From)
		}
	case *events.CallTerminate:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTerminate(wrapCall(evt.From, evt.Data))
		}
	case *events.CallReject:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTerminate(wrapCall(evt.From, evt.Data))
		}
	}
}

func (s *Session) connect(ctx context.Context) error {
	if s.client.Store.ID != nil {
		return s.client.Connect()
	}
	return s.startPairing(ctx)
}

func (s *Session) startPairing(ctx context.Context) error {
	qrChan, err := s.client.GetQRChannel(ctx)
	if err != nil {
		return err
	}
	if err := s.client.Connect(); err != nil {
		return err
	}
	go func() {
		for evt := range qrChan {
			switch evt.Event {
			case "code":
				s.log.Info("scan the QR code to pair this session")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				s.setAuth(AuthSnapshot{State: "qr", QR: evt.Code})
				s.mgr.broker.emitSessionQR(s.id, evt.Code)
			case "success":
				if id := s.client.Store.ID; id != nil {
					_ = s.mgr.store.setJID(s.mgr.appCtx, s.id, id.String())
				}
				s.setAuth(AuthSnapshot{State: "open", Paired: true})
			case "timeout":
				s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
			}
		}
	}()
	return nil
}

func (s *Session) setAuth(a AuthSnapshot) {
	s.mu.Lock()
	s.auth = a
	s.mu.Unlock()
	s.mgr.broker.emitAuthState(s.id, a)
	s.mgr.broker.emitSessionList(s.mgr.infos())
}

func (s *Session) info() SessionInfo {
	s.mu.Lock()
	a := s.auth
	s.mu.Unlock()
	jid := ""
	if id := s.client.Store.ID; id != nil {
		jid = id.String()
	}
	return SessionInfo{ID: s.id, Name: s.name, JID: jid, State: a.State, Paired: a.Paired || jid != "", QR: a.QR}
}

func (s *Session) setBridge(callID string, b *Bridge) {
	oldB, found := s.reg.setBridge(callID, b)
	if !found {
		b.Close()
		return
	}
	if oldB != nil {
		oldB.Close()
	}
}

func (s *Session) removeCall(callID string) {
	ac, ok := s.reg.remove(callID)
	if !ok {
		return
	}
	if ac.bridge != nil {
		ac.bridge.Close()
	}
	// Re-anuncia presença ativa ao fim da chamada: é logo APÓS a 1ª ligação que
	// o WA costuma marcar o companheiro (coex) como uncallable. Re-afirmar aqui
	// mantém as próximas chamadas tocando.
	go s.markAvailable(context.Background())
}

// isSelfDevice: true quando o JID é de um device da NOSSA PRÓPRIA conta (mesmo
// user do nosso PN ou LID), ex.: o lado hosted da coex "…:99@hosted.lid".
func (s *Session) isSelfDevice(from types.JID) bool {
	if s.client == nil || s.client.Store == nil {
		return false
	}
	u := from.User
	if u == "" {
		return false
	}
	if lid := s.client.Store.LID; lid.User != "" && lid.User == u {
		return true
	}
	if id := s.client.Store.ID; id != nil && id.User == u {
		return true
	}
	return false
}

// markAvailable anuncia presença "available" (mantém o device callable no WA).
// SendPresence exige PushName não-vazio — companheiro às vezes vem sem, então
// setamos um nome estável antes de anunciar.
func (s *Session) markAvailable(ctx context.Context) {
	if s.client == nil || s.client.Store == nil || s.client.Store.ID == nil {
		return
	}
	if s.client.Store.PushName == "" {
		name := s.name
		if name == "" {
			name = "WaCalls"
		}
		s.client.Store.PushName = name
	}
	if err := s.client.SendPresence(ctx, types.PresenceAvailable); err != nil {
		s.log.Warn("presence available falhou", "err", err)
		return
	}
	s.log.Info("🟢 presença available anunciada (mantém companheiro callable)")
}

// startPresenceKeepalive re-anuncia presença periodicamente (o WA expira a
// presença ativa por inatividade). 1 goroutine por sessão.
func (s *Session) startPresenceKeepalive() {
	s.presenceOnce.Do(func() {
		go func() {
			t := time.NewTicker(4 * time.Minute)
			defer t.Stop()
			for range t.C {
				if s.client == nil || !s.client.IsConnected() {
					continue
				}
				s.markAvailable(context.Background())
			}
		}()
	})
}

func (s *Session) terminateCall(callID string, reason core.EndCallReason) {
	ac, ok := s.reg.get(callID)
	if !ok {
		return
	}
	_ = ac.cm.EndCall(context.Background(), reason)
}

func (s *Session) teardownAllCalls() {
	for _, ac := range s.reg.drain() {
		_ = ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
		if ac.bridge != nil {
			ac.bridge.Close()
		}
	}
}

func (s *Session) replaceClient(client *whatsmeow.Client) {
	s.teardownAllCalls()
	s.client.Disconnect()
	s.client = client
	client.AddEventHandler(s.handleEvent)
}

func (s *Session) shutdown() {
	s.teardownAllCalls()
	s.client.Disconnect()
}

func mapStatus(state core.CallState) CallStatus {
	switch state {
	case core.CallStateActive:
		return StatusConnected
	case core.CallStateEnded:
		return StatusEnded
	case core.CallStateInitiating:
		return StatusStarting
	default:
		return StatusRinging
	}
}
