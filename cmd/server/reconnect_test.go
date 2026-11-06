package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type fakeConn struct {
	connected bool
	err       error
	dials     int
}

func (f *fakeConn) IsConnected() bool { return f.connected }
func (f *fakeConn) Connect() error    { f.dials++; return f.err }

func keepWAVersion(t *testing.T) wastore.WAVersionContainer {
	t.Helper()
	original := wastore.GetWAVersion()
	t.Cleanup(func() { wastore.SetWAVersion(original) })
	return original
}

func TestDropEventsMarkSessionDownAndScheduleRetry(t *testing.T) {
	cases := []struct {
		name  string
		event any
		delay time.Duration
		stale bool
	}{
		{"queda de rede tenta logo", &events.Disconnected{}, 0, false},
		{"conexão substituída não disputa", &events.StreamReplaced{}, replacedRetryDelay, false},
		{"versão velha pede versão nova", &events.ClientOutdated{}, outdatedRetryDelay, true},
		{"falha de conexão", &events.ConnectFailure{Reason: 500, Message: "x"}, failureRetryDelay, false},
		{"ban curto usa o mínimo", &events.TemporaryBan{Expire: time.Minute}, minBanRetryDelay, false},
		{"ban longo respeita o prazo", &events.TemporaryBan{Expire: 2 * time.Hour}, 2 * time.Hour, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := newTestManager(t)
			s := m.addUnconnected(t, "A")
			s.setAuth(AuthSnapshot{State: "open", Paired: true})

			before := time.Now()
			s.handleEvent(c.event)

			if s.auth.State != "connecting" {
				t.Fatalf("estado = %q, esperava connecting", s.auth.State)
			}
			if got := s.retryAt.Sub(before); got < c.delay || got > c.delay+5*time.Second {
				t.Fatalf("nova tentativa em %v, esperava %v", got, c.delay)
			}
			if m.versionStale.Load() != c.stale {
				t.Fatalf("versionStale = %v, esperava %v", m.versionStale.Load(), c.stale)
			}
		})
	}
}

func TestConnectedClearsRetry(t *testing.T) {
	m := newTestManager(t)
	s := m.addUnconnected(t, "A")
	s.retryAt = time.Now().Add(time.Hour)

	s.handleEvent(&events.Connected{})

	if !s.retryAt.IsZero() {
		t.Fatalf("retryAt = %v, esperava zerado", s.retryAt)
	}
	if s.auth.State != "open" {
		t.Fatalf("estado = %q, esperava open", s.auth.State)
	}
}

func TestInfoDoesNotReportOpenWithoutConnection(t *testing.T) {
	m := newTestManager(t)
	s := m.addUnconnected(t, "A")
	s.setAuth(AuthSnapshot{State: "open", Paired: true})

	if got := s.info().State; got != "connecting" {
		t.Fatalf("info().State = %q, esperava connecting", got)
	}
}

func TestTryReconnect(t *testing.T) {
	now := time.Now()
	m := newTestManager(t)
	s := m.addUnconnected(t, "A")

	up := &fakeConn{connected: true}
	if s.tryReconnect(up, now) || up.dials != 0 {
		t.Fatal("sessão conectada não pode discar")
	}

	s.retryAt = now.Add(time.Minute)
	waiting := &fakeConn{}
	if s.tryReconnect(waiting, now) || waiting.dials != 0 {
		t.Fatal("não pode discar antes do prazo")
	}

	due := &fakeConn{}
	if !s.tryReconnect(due, now.Add(2*time.Minute)) || due.dials != 1 {
		t.Fatalf("depois do prazo deveria discar 1 vez, discou %d", due.dials)
	}

	s.clearRetry()
	failing := &fakeConn{err: errors.New("rede")}
	if !s.tryReconnect(failing, now) || failing.dials != 1 {
		t.Fatal("falha conta como tentativa")
	}

	raced := &fakeConn{err: whatsmeow.ErrAlreadyConnected}
	if s.tryReconnect(raced, now) {
		t.Fatal("ErrAlreadyConnected não conta como tentativa")
	}
}

func TestReconnectSkipsUnpairedSession(t *testing.T) {
	m := newTestManager(t)
	s := m.addUnconnected(t, "A")
	if s.reconnectIfDropped(time.Now()) {
		t.Fatal("sessão sem pareamento precisa de QR, não de reconexão")
	}
}

func TestRefreshWAVersion(t *testing.T) {
	original := keepWAVersion(t)
	m := newTestManager(t)
	bump := func(v wastore.WAVersionContainer, by int32) wastore.WAVersionContainer {
		v[2] = uint32(int64(v[2]) + int64(by))
		return v
	}

	m.fetchVersion = func(context.Context) (*wastore.WAVersionContainer, error) { return nil, errors.New("fora do ar") }
	if m.refreshWAVersion(context.Background()) || wastore.GetWAVersion() != original {
		t.Fatal("falha ao buscar deve manter a versão em uso")
	}

	same := original
	m.fetchVersion = func(context.Context) (*wastore.WAVersionContainer, error) { return &same, nil }
	if m.refreshWAVersion(context.Background()) {
		t.Fatal("mesma versão não conta como mudança")
	}

	older := bump(original, -1)
	m.fetchVersion = func(context.Context) (*wastore.WAVersionContainer, error) { return &older, nil }
	if m.refreshWAVersion(context.Background()) || wastore.GetWAVersion() != original {
		t.Fatal("não pode voltar para versão mais antiga")
	}

	newer := bump(original, 1)
	m.fetchVersion = func(context.Context) (*wastore.WAVersionContainer, error) { return &newer, nil }
	if !m.refreshWAVersion(context.Background()) || wastore.GetWAVersion() != newer {
		t.Fatal("versão nova deveria ser aplicada")
	}
}

func TestReconnectDroppedRefreshesVersionAndReleasesWaiting(t *testing.T) {
	original := keepWAVersion(t)
	m := newTestManager(t)
	s := m.addUnconnected(t, "A")
	s.handleEvent(&events.ClientOutdated{})

	newer := original
	newer[2]++
	fetches := 0
	m.fetchVersion = func(context.Context) (*wastore.WAVersionContainer, error) { fetches++; return &newer, nil }

	m.reconnectDropped(context.Background(), time.Now())

	if fetches != 1 || wastore.GetWAVersion() != newer {
		t.Fatalf("deveria buscar 1 vez e aplicar: buscas=%d versão=%v", fetches, wastore.GetWAVersion())
	}
	if !s.retryAt.IsZero() {
		t.Fatal("versão nova deveria liberar a sessão na hora")
	}
	if m.versionStale.Load() {
		t.Fatal("o aviso de versão velha deveria ter sido consumido")
	}

	m.reconnectDropped(context.Background(), time.Now())
	if fetches != 1 {
		t.Fatal("sem novo 405 não busca a versão de novo")
	}
}

func TestStartCallRefusesWhenSessionIsDown(t *testing.T) {
	m := newTestManager(t)
	s := m.addUnconnected(t, "A")
	s.client.Store.ID = &types.JID{User: "5500000000000", Server: types.DefaultUserServer}
	srv := &server{broker: m.broker, sessions: m}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/x/calls", strings.NewReader(`{"phone":"5500000000000"}`))
	rec := httptest.NewRecorder()
	srv.doStartCall(s, rec, req)

	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "session not connected") {
		t.Fatalf("resposta = %d %s", rec.Code, rec.Body.String())
	}
}
