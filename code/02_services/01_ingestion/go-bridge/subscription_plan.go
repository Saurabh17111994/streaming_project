package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

const (
	MaxHFTTokensPerConnection = 1024
	MaxHFTTokensPerRequest    = 512
	MaxHFTConnections         = 3
)

type SlotAssignment struct {
	SlotID       string
	ConnectionID string
	Tokens       []int32
	Requests     [][]int32
}

type SubscriptionPlan struct {
	Slots       []SlotAssignment
	Fingerprint string
}

func BuildSubscriptionPlan(tokens []int32, slots, connectionLimit, requestLimit int) (SubscriptionPlan, error) {
	if len(tokens) == 0 {
		return SubscriptionPlan{}, fmt.Errorf("instrument token input must not be empty")
	}
	if slots <= 0 || slots > MaxHFTConnections {
		return SubscriptionPlan{}, fmt.Errorf("slot count must be between 1 and %d", MaxHFTConnections)
	}
	if connectionLimit <= 0 || connectionLimit > MaxHFTTokensPerConnection {
		return SubscriptionPlan{}, fmt.Errorf("connection limit must be between 1 and %d", MaxHFTTokensPerConnection)
	}
	if requestLimit <= 0 || requestLimit > MaxHFTTokensPerRequest {
		return SubscriptionPlan{}, fmt.Errorf("request limit must be between 1 and %d", MaxHFTTokensPerRequest)
	}
	if len(tokens) > slots*connectionLimit {
		return SubscriptionPlan{}, fmt.Errorf("token count %d exceeds capacity %d", len(tokens), slots*connectionLimit)
	}
	ordered := append([]int32(nil), tokens...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for i := 1; i < len(ordered); i++ {
		if ordered[i] == ordered[i-1] {
			return SubscriptionPlan{}, fmt.Errorf("duplicate token %d", ordered[i])
		}
	}
	plan := SubscriptionPlan{}
	for i := 0; i < slots; i++ {
		start := i * connectionLimit
		if start >= len(ordered) {
			break
		}
		end := start + connectionLimit
		if end > len(ordered) {
			end = len(ordered)
		}
		assignment := SlotAssignment{SlotID: fmt.Sprintf("hft-%d", i), ConnectionID: fmt.Sprintf("hft-%d", i), Tokens: append([]int32(nil), ordered[start:end]...)}
		for j := 0; j < len(assignment.Tokens); j += requestLimit {
			k := j + requestLimit
			if k > len(assignment.Tokens) {
				k = len(assignment.Tokens)
			}
			assignment.Requests = append(assignment.Requests, append([]int32(nil), assignment.Tokens[j:k]...))
		}
		plan.Slots = append(plan.Slots, assignment)
	}
	h := sha256.New()
	for _, slot := range plan.Slots {
		fmt.Fprintf(h, "%s|%s|", slot.SlotID, slot.ConnectionID)
		for _, token := range slot.Tokens {
			fmt.Fprintf(h, "%d,", token)
		}
	}
	plan.Fingerprint = hex.EncodeToString(h.Sum(nil))
	return plan, nil
}
