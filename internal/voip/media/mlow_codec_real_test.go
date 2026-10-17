//go:build mlow

package media

import (
	"math"
	"sync"
	"testing"
)

func TestMLowCodecRoundtrip(t *testing.T) {
	codec, err := NewMLowCodec(DefaultCodecOptions)
	if err != nil {
		t.Fatalf("NewMLowCodec: %v", err)
	}
	defer codec.Close()

	if codec.FrameSize() != 960 || codec.SampleRate() != 16000 {
		t.Fatalf("unexpected frame=%d rate=%d", codec.FrameSize(), codec.SampleRate())
	}

	frame := make([]float32, 960)
	for i := range frame {
		frame[i] = 0.3 * float32(math.Sin(2*math.Pi*440*float64(i)/16000))
	}

	encoded, err := codec.Encode(frame)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("encoded frame is empty")
	}
	t.Logf("encoded %d samples → %d bytes (MLow)", len(frame), len(encoded))

	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(decoded) == 0 {
		t.Fatal("decoded PCM is empty")
	}
	t.Logf("decoded → %d samples", len(decoded))

	plc, err := codec.Decode(nil)
	if err != nil {
		t.Fatalf("Decode(nil) PLC: %v", err)
	}
	if len(plc) == 0 {
		t.Fatal("PLC returned no samples")
	}
}

// TestMLowCodecConcurrentDecode reproduz o padrão observado em produção: em
// chamadas com vários relés ICE ativos ao mesmo tempo (redundância de
// conectividade), cada relé entrega áudio numa goroutine própria e chama
// Decode() na MESMA instância de codec concorrentemente. Sem o mutex em
// mlowCodec, isso corrompe o estado do resampler SILK (libopus_mlow não é
// thread-safe) e aborta o processo inteiro com "assertion failed: 0" em
// silk/resampler.c:101 — SIGABRT não dá pra recuperar nem com recover() em
// Go. Roda sob -race pra pegar qualquer acesso concorrente não serializado.
func TestMLowCodecConcurrentDecode(t *testing.T) {
	codec, err := NewMLowCodec(DefaultCodecOptions)
	if err != nil {
		t.Fatalf("NewMLowCodec: %v", err)
	}
	defer codec.Close()

	frame := make([]float32, 960)
	for i := range frame {
		frame[i] = 0.3 * float32(math.Sin(2*math.Pi*440*float64(i)/16000))
	}
	encoded, err := codec.Encode(frame)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	const goroutines = 8
	const itersPerGoroutine = 50
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*itersPerGoroutine)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < itersPerGoroutine; i++ {
				if _, err := codec.Decode(encoded); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Decode: %v", err)
	}
}

// TestMLowCodecConcurrentDecodeCrossInstance reproduz a 2ª ocorrência em
// produção: duas SESSÕES diferentes (duas linhas WhatsApp do mesmo tenant,
// cada uma com seu próprio CallManager/mlowCodec — callRegistry é por sessão,
// sem deduplicação entre sessões) processaram "remote accepted call" pro
// MESMO call_id quase ao mesmo tempo e decodificaram áudio concorrentemente
// em INSTÂNCIAS DIFERENTES de mlowCodec. O mutex por-instância (1ª correção)
// não cobre isso — só opusMu (global, package-level) cobre.
func TestMLowCodecConcurrentDecodeCrossInstance(t *testing.T) {
	const numCodecs = 4
	codecs := make([]Codec, numCodecs)
	encoded := make([][]byte, numCodecs)
	for i := range codecs {
		c, err := NewMLowCodec(DefaultCodecOptions)
		if err != nil {
			t.Fatalf("NewMLowCodec[%d]: %v", i, err)
		}
		codecs[i] = c
		defer c.Close()

		frame := make([]float32, 960)
		for s := range frame {
			frame[s] = 0.3 * float32(math.Sin(2*math.Pi*440*float64(s)/16000))
		}
		enc, err := c.Encode(frame)
		if err != nil {
			t.Fatalf("Encode[%d]: %v", i, err)
		}
		encoded[i] = enc
	}

	const itersPerCodec = 50
	var wg sync.WaitGroup
	errs := make(chan error, numCodecs*itersPerCodec)
	for i := range codecs {
		wg.Add(1)
		go func(codec Codec, frame []byte) {
			defer wg.Done()
			for j := 0; j < itersPerCodec; j++ {
				if _, err := codec.Decode(frame); err != nil {
					errs <- err
				}
			}
		}(codecs[i], encoded[i])
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("cross-instance concurrent Decode: %v", err)
	}
}
