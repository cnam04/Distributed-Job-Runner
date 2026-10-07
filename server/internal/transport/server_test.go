package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeBindErrors(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	for _, addr := range []string{listener.Addr().String(), "127.0.0.1:invalid-port"} {
		t.Run(addr, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := Serve(ctx, addr, http.NewServeMux())
			var listenErr *net.OpError
			if !errors.As(err, &listenErr) || listenErr.Op != "listen" {
				t.Errorf("Serve error = %v, want listen error", err)
			}
		})
	}
}

func TestServeCanceledContext(t *testing.T) {
	testCtx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	ctx, cancel := context.WithCancel(testCtx)
	cancel()
	result := make(chan error, 1)
	go func() { result <- Serve(ctx, "127.0.0.1:0", http.NewServeMux()) }()
	select {
	case err := <-result:
		if err != nil {
			t.Errorf("canceled Serve error = %v, want clean shutdown", err)
		}
	case <-testCtx.Done():
		t.Fatal("Serve did not shut down after cancellation")
	}
}
