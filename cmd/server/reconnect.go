package main

import (
	"context"
	"errors"
	"time"

	"go.mau.fi/whatsmeow"
	wastore "go.mau.fi/whatsmeow/store"
)

// O whatsmeow só reconecta sozinho quando a queda é de rede. Quando o WhatsApp recusa a
// sessão (versão velha, banimento temporário, conexão substituída) ele desiste, e nada
// tentava de novo: a sessão seguia "open" e toda ligação falhava até reiniciar a máquina.
// Foi o que derrubou as ligações em 10/2026: a versão fixa na biblioteca venceu (405).
const (
	reconnectCheckInterval = time.Minute
	outdatedRetryDelay     = 5 * time.Minute
	replacedRetryDelay     = 10 * time.Minute // outro processo usa as mesmas chaves: não disputar
	failureRetryDelay      = 5 * time.Minute
	minBanRetryDelay       = 10 * time.Minute
	// Curto de propósito: roda antes de o /healthz subir. Se falhar, o 1º 405 busca de novo.
	versionFetchTimeout = 5 * time.Second
)

type versionFetcher func(ctx context.Context) (*wastore.WAVersionContainer, error)

func fetchLatestWAVersion(ctx context.Context) (*wastore.WAVersionContainer, error) {
	ctx, cancel := context.WithTimeout(ctx, versionFetchTimeout)
	defer cancel()
	return whatsmeow.GetLatestVersion(ctx, nil)
}

// refreshWAVersion aplica às próximas conexões a versão publicada pelo WhatsApp e diz se
// mudou. Nunca volta para uma versão mais antiga do que a que já está em uso.
func (m *SessionManager) refreshWAVersion(ctx context.Context) bool {
	current := wastore.GetWAVersion()
	latest, err := m.fetchVersion(ctx)
	if err != nil {
		m.log.Warn("não obteve a versão atual do WhatsApp", "em_uso", current.String(), "err", err)
		return false
	}
	if *latest == current || latest.LessThan(current) {
		return false
	}
	wastore.SetWAVersion(*latest)
	m.log.Info("versão do WhatsApp atualizada", "de", current.String(), "para", latest.String())
	return true
}

func (m *SessionManager) runReconnectLoop(ctx context.Context) {
	t := time.NewTicker(reconnectCheckInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			m.reconnectDropped(ctx, now)
		}
	}
}

func (m *SessionManager) reconnectDropped(ctx context.Context, now time.Time) {
	sessions := m.all()
	// Versão nova libera na hora quem estava esperando por causa da versão velha.
	if m.versionStale.Swap(false) && m.refreshWAVersion(ctx) {
		for _, s := range sessions {
			s.clearRetry()
		}
	}
	for _, s := range sessions {
		if ctx.Err() != nil {
			return
		}
		s.reconnectIfDropped(now)
	}
}

// reconnectIfDropped só age em sessão pareada: sem pareamento, a saída é um QR novo.
func (s *Session) reconnectIfDropped(now time.Time) bool {
	client := s.client
	if client == nil || client.Store == nil || client.Store.ID == nil {
		return false
	}
	return s.tryReconnect(client, now)
}

type waConnection interface {
	IsConnected() bool
	Connect() error
}

func (s *Session) tryReconnect(conn waConnection, now time.Time) bool {
	if conn.IsConnected() {
		return false
	}
	s.mu.Lock()
	waiting := now.Before(s.retryAt)
	s.mu.Unlock()
	if waiting {
		return false
	}
	if err := conn.Connect(); err != nil {
		// O auto-reconnect do whatsmeow pode ter chegado antes: não é falha.
		if errors.Is(err, whatsmeow.ErrAlreadyConnected) {
			return false
		}
		s.log.Warn("reconexão falhou", "err", err)
		return true
	}
	s.log.Info("reconexão iniciada")
	return true
}

func (s *Session) clearRetry() {
	s.mu.Lock()
	s.retryAt = time.Time{}
	s.mu.Unlock()
}

// markDown registra por que a sessão caiu e reflete isso no estado. Antes, só LoggedOut
// mudava o estado e o motivo de qualquer outra queda não aparecia em lugar nenhum.
func (s *Session) markDown(reason string, retryIn time.Duration, attrs ...any) {
	s.mu.Lock()
	s.retryAt = time.Now().Add(retryIn)
	s.mu.Unlock()
	s.log.Warn("sessão desconectada do WhatsApp", append([]any{"motivo", reason, "nova_tentativa_em", retryIn}, attrs...)...)
	s.setAuth(AuthSnapshot{State: "connecting", Paired: true})
}

func banRetryDelay(expire time.Duration) time.Duration {
	if expire > minBanRetryDelay {
		return expire
	}
	return minBanRetryDelay
}
