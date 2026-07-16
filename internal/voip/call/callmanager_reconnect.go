package call

import (
	"context"
	"time"

	"wacalls/internal/voip/core"
)

// reconnectGracePeriod — quanto tempo a chamada fica em "reconnecting" antes de desistir
// e encerrar. Perda transitória de relay/rede costuma voltar em poucos segundos; se
// passar disso, a mídia provavelmente não volta.
const reconnectGracePeriod = 20 * time.Second

// onRelayUsableChange é chamado pelo transporte quando o nº de relays ABERTOS muda.
// Detecta perda de mídia (usable==0 numa chamada ativa) e restauração (usable>0 numa
// chamada em reconnecting). É idempotente: só age no cruzamento das fronteiras 0↔>0.
//
// Ortogonal ao coex: o gatilho é o NOSSO ICE/relay (caminho de mídia), não sinais de
// device irmão — os guards de coex (isSelfDevice em terminate/reject/accept) seguem intactos.
func (m *CallManager) onRelayUsableChange(usable int) {
	m.mu.Lock()
	call := m.currentCall
	if call == nil {
		m.mu.Unlock()
		return
	}
	state := call.StateData.State

	switch {
	case usable == 0 && state == core.CallStateActive:
		// Mídia perdida: entra em reconnecting, arma o grace timer e tenta re-discar os relays.
		if err := call.ApplyTransition(Transition{Type: TransitionMediaLost}); err != nil {
			m.mu.Unlock()
			return
		}
		var endpoints []core.RelayEndpoint
		if call.RelayData != nil {
			endpoints = append(endpoints, call.RelayData.Endpoints...)
		}
		callID := call.CallID
		m.startReconnectTimerLocked(callID)
		m.emitState()
		m.log.Warn("media lost → reconnecting", "call_id", callID, "grace", reconnectGracePeriod.String())
		m.mu.Unlock()
		// recycle FORA do lock (ConfigureRelays disca em goroutines). Os relays que caíram
		// foram removidos do map, então connectRelays re-disca só os ausentes.
		if len(endpoints) > 0 {
			go m.connectRelays(endpoints)
		}
		return

	case usable > 0 && state == core.CallStateReconnecting:
		// Mídia restaurada: volta pra active, cancela o grace timer e reassina as subscriptions.
		if err := call.ApplyTransition(Transition{Type: TransitionMediaRestored}); err != nil {
			m.mu.Unlock()
			return
		}
		m.stopReconnectTimerLocked()
		m.emitState()
		// O send loop persiste durante o reconnect (idle enquanto HasConnection==false) e
		// auto-retoma; chamamos startMediaSendLoopLocked por segurança (é no-op se já rodando).
		m.startMediaSendLoopLocked()
		callID := call.CallID
		m.log.Info("media restored → active", "call_id", callID)
		m.mu.Unlock()
		// reassina as subscriptions no relay novo p/ o peer voltar a encaminhar áudio.
		m.relay.ResendSubscriptions()
		return
	}

	m.mu.Unlock()
}

// DebugDropRelays fecha NOSSOS relays de propósito p/ exercitar a reconexão de forma
// determinística (endpoint de debug). Só age se houver chamada ativa. Retorna quantos
// relays foram fechados (0 = sem chamada/relay). Dispara onRelayUsableChange(0) →
// media lost → reconnecting → re-disca → media restored.
func (m *CallManager) DebugDropRelays() int {
	m.mu.Lock()
	call := m.currentCall
	m.mu.Unlock()
	if call == nil {
		return 0
	}
	n := m.relay.DropAllForDebug()
	m.log.Warn("🧪 [DEBUG] relays fechados manualmente p/ testar reconexão", "call_id", call.CallID, "dropped", n)
	return n
}

// startReconnectTimerLocked arma (ou re-arma) o grace timer. Chamado com m.mu travado.
func (m *CallManager) startReconnectTimerLocked(callID string) {
	if m.reconnectTimer != nil {
		m.reconnectTimer.Stop()
	}
	m.reconnectTimer = time.AfterFunc(reconnectGracePeriod, func() {
		m.mu.Lock()
		call := m.currentCall
		// só encerra se AINDA for a mesma chamada e AINDA estiver reconnecting
		if call == nil || call.CallID != callID || call.StateData.State != core.CallStateReconnecting {
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
		m.log.Warn("reconnect grace expired → ending call", "call_id", callID)
		// razão 'failed' (mídia caiu e não voltou) — distinta do watchdog de no-answer (timeout).
		_ = m.EndCall(context.Background(), core.EndCallReasonFailed)
	})
}

// stopReconnectTimerLocked cancela o grace timer. Chamado com m.mu travado.
func (m *CallManager) stopReconnectTimerLocked() {
	if m.reconnectTimer != nil {
		m.reconnectTimer.Stop()
		m.reconnectTimer = nil
	}
}
