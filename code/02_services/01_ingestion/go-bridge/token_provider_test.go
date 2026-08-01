package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenProviderRefresh(t *testing.T) {
	p, err := NewArrowTokenProvider("old", func(context.Context) (string, error) { return "new", nil })
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := p.Refresh(context.Background()); got != "new" {
		t.Fatalf("got %q", got)
	}
	if _, err := p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTokenProviderRefreshHonorsCancellation(t *testing.T) {
	p, err := NewArrowTokenProvider("old", func(context.Context) (string, error) { return "new", nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Refresh(ctx); err == nil {
		t.Fatal("expected cancellation error")
	}
}

func TestTokenProviderSerializesConcurrentRefresh(t *testing.T) {
	var active, maxActive int32
	var mu sync.Mutex
	p, err := NewArrowTokenProvider("old", func(context.Context) (string, error) {
		n := atomic.AddInt32(&active, 1)
		mu.Lock()
		if n > maxActive {
			maxActive = n
		}
		mu.Unlock()
		time.Sleep(time.Millisecond)
		atomic.AddInt32(&active, -1)
		return "new", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = p.Refresh(context.Background()) }()
	}
	wg.Wait()
	if maxActive != 1 {
		t.Fatalf("max concurrent refreshes=%d, want 1", maxActive)
	}
}
