package main

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// iceServersFromEnv monta a lista de ICE servers a partir de env:
//   WACALLS_STUN_URLS      — CSV de urls stun:
//   WACALLS_TURN_URLS      — CSV de urls turn: (opcional)
//   WACALLS_TURN_USERNAME  — usuário do TURN
//   WACALLS_TURN_CREDENTIAL— credencial do TURN
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

type Bridge struct {
	pc         *webrtc.PeerConnection
	localTrack *webrtc.TrackLocalStaticSample
	log        *slog.Logger

	OnBrowserRTP  func(payload []byte)
	OnTerminalICE func()
}

func NewBridge(offerSDP string, log *slog.Logger) (*Bridge, string, error) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: iceServersFromEnv()})
	if err != nil {
		return nil, "", err
	}

	br := &Bridge{pc: pc, log: log}

	localTrack, err := webrtc.NewTrackLocalStaticSample(

		webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		},
		"audio", "wacalls",
	)
	if err != nil {
		pc.Close()
		return nil, "", err
	}
	br.localTrack = localTrack

	pc.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go func() {
			for {
				pkt, _, err := tr.ReadRTP()
				if err != nil {
					return
				}
				if br.OnBrowserRTP != nil && len(pkt.Payload) > 0 {
					br.OnBrowserRTP(pkt.Payload)
				}
			}
		}()
	})

	pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		log.Debug("browser ice state", "state", s.String())
		if s == webrtc.ICEConnectionStateFailed || s == webrtc.ICEConnectionStateClosed {
			if br.OnTerminalICE != nil {
				br.OnTerminalICE()
			}
		}
	})

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offerSDP}); err != nil {
		pc.Close()
		return nil, "", err
	}
	if _, err := pc.AddTrack(localTrack); err != nil {
		pc.Close()
		return nil, "", err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		pc.Close()
		return nil, "", err
	}
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		pc.Close()
		return nil, "", err
	}
	<-gatherComplete

	return br, pc.LocalDescription().SDP, nil
}

func (b *Bridge) WriteOpus(payload []byte, dur time.Duration) error {
	if b.localTrack == nil {
		return nil
	}
	return b.localTrack.WriteSample(media.Sample{Data: payload, Duration: dur})
}

func (b *Bridge) Close() {
	if b.pc != nil {
		_ = b.pc.Close()
	}
}
