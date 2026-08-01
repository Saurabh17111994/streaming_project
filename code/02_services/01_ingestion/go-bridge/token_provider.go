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
	mu        sync.Mutex
	current   string
	refreshFn func(context.Context) (string, error)
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
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current, nil
}

func (p *ArrowTokenProvider) Refresh(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
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
	p.current = token
	return token, nil
}
