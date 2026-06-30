package main

import (
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

var (
	browserAPIOnce sync.Once
	browserAPI     *webrtc.API
	browserAPIErr  error
)

// browserWebRTCAPI returns a process-wide *webrtc.API for the browser-facing
// PeerConnections. When WACALLS_WEBRTC_UDP_PORT is set, all ICE traffic is
// funneled through a single fixed UDP port and host candidates are published
// with WACALLS_PUBLIC_IP, so the server is reachable behind a 1:1 NAT such as a
// Docker bridge. Without the env vars it falls back to pion's default behavior
// (ephemeral ports, interface IPs) used for local/LAN runs.
func browserWebRTCAPI() (*webrtc.API, error) {
	browserAPIOnce.Do(func() {
		port, _ := strconv.Atoi(strings.TrimSpace(os.Getenv("WACALLS_WEBRTC_UDP_PORT")))
		browserAPI, browserAPIErr = buildBrowserAPI(port, publicIPs())
	})
	return browserAPI, browserAPIErr
}

func buildBrowserAPI(udpPort int, externalIPs []string) (*webrtc.API, error) {
	if udpPort <= 0 {
		return webrtc.NewAPI(), nil
	}

	mux, err := ice.NewMultiUDPMuxFromPort(udpPort, ice.UDPMuxFromPortWithNetworks(ice.NetworkTypeUDP4))
	if err != nil {
		return nil, err
	}

	se := webrtc.SettingEngine{}
	se.SetICEUDPMux(mux)
	if len(externalIPs) > 0 {
		if err := se.SetICEAddressRewriteRules(webrtc.ICEAddressRewriteRule{
			External:        externalIPs,
			AsCandidateType: webrtc.ICECandidateTypeHost,
		}); err != nil {
			return nil, err
		}
	}
	return webrtc.NewAPI(webrtc.WithSettingEngine(se)), nil
}

func publicIPs() []string {
	raw := strings.TrimSpace(os.Getenv("WACALLS_PUBLIC_IP"))
	if raw == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// iceServersFromEnv monta a lista de ICE servers (STUN/TURN) a partir de env.
// Complementa o SettingEngine (UDP fixo / WACALLS_PUBLIC_IP): aqui são os
// servidores STUN/TURN que o browser usa p/ atravessar NAT.
//   WACALLS_STUN_URLS       — CSV de urls stun:
//   WACALLS_TURN_URLS       — CSV de urls turn: (opcional)
//   WACALLS_TURN_USERNAME   — usuário do TURN
//   WACALLS_TURN_CREDENTIAL — credencial do TURN
func iceServersFromEnv() []webrtc.ICEServer {
	var servers []webrtc.ICEServer
	if urls := splitCSV(os.Getenv("WACALLS_STUN_URLS")); len(urls) > 0 {
		servers = append(servers, webrtc.ICEServer{URLs: urls})
	}
	if urls := splitCSV(os.Getenv("WACALLS_TURN_URLS")); len(urls) > 0 {
		servers = append(servers, webrtc.ICEServer{
			URLs:       urls,
			Username:   os.Getenv("WACALLS_TURN_USERNAME"),
			Credential: os.Getenv("WACALLS_TURN_CREDENTIAL"),
		})
	}
	return servers
}

// splitCSV divide por vírgula, faz trim e descarta itens vazios.
func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
