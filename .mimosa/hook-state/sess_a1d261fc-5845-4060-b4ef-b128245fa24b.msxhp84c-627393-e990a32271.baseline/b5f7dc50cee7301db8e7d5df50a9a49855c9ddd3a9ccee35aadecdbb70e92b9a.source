package main

import (
	"context"
	"fmt"
	"sync"
)

type TokenProvider interface {
	Current(context.Context) (string, error)
	Refresh(context.Context) (string, error)
}

type ArrowTokenProvider struct {
	mu        sync.RWMutex
	current   string
	refreshFn func(context.Context) (string, error)
	// refreshMu serializes refreshFn (R-139): only one network refresh runs at
	// a time, while Current() readers never block on it.
	refreshMu sync.Mutex
}

func NewArrowTokenProvider(initial string, refreshFn func(context.Context) (string, error)) (*ArrowTokenProvider, error) {
	if initial == "" {
		return nil, fmt.Errorf("initial token is required")
	}
	if refreshFn == nil {
		return nil, fmt.Errorf("refresh function is required")
	}
	return &ArrowTokenProvider{current: initial, refreshFn: refreshFn}, nil
}

func (p *ArrowTokenProvider) Current(context.Context) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.current, nil
}

func (p *ArrowTokenProvider) Refresh(ctx context.Context) (string, error) {
	// R-139: refreshFn typically performs network I/O. The old code held the
	// exclusive lock across it, blocking every Current() caller and queued
	// Refresh callers for the whole refresh window. refreshMu serializes the
	// network call (one refresh at a time) but Current() readers use RLock and
	// never wait on it.
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	token, err := p.refreshFn(ctx)
	if err != nil {
		return "", err
	}
	if token == "" {
		return "", fmt.Errorf("refresh returned empty token")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = token
	return token, nil
}
