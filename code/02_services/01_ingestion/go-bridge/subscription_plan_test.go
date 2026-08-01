package main

import "testing"

func TestSubscriptionPlanBoundaries(t *testing.T) {
	for _, n := range []int{1, 512, 513, 1024, 1025, 2048, 3000, 3072} {
		tokens := make([]int32, n)
		for i := range tokens {
			tokens[i] = int32(n - i)
		}
		plan, err := BuildSubscriptionPlan(tokens, 3, 1024, 512)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		seen := map[int32]bool{}
		for _, slot := range plan.Slots {
			for _, token := range slot.Tokens {
				if seen[token] {
					t.Fatalf("duplicate %d", token)
				}
				seen[token] = true
			}
		}
		if len(seen) != n {
			t.Fatalf("n=%d got %d", n, len(seen))
		}
	}
	if _, err := BuildSubscriptionPlan(make([]int32, 3073), 3, 1024, 512); err == nil {
		t.Fatal("expected capacity rejection")
	}
	if _, err := BuildSubscriptionPlan([]int32{1, 1}, 1, 1024, 512); err == nil {
		t.Fatal("expected duplicate rejection")
	}
}

// ING-CAP-001 — 512 and 1024 manifests report exact capacity (request chunks
// and slots); an over-capacity manifest never starts.
func TestIngCap001CapacityAccounting(t *testing.T) {
	// 512 tokens on one connection → exactly one request of 512.
	plan, err := BuildSubscriptionPlan(ids(512), 1, 1024, 512)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Slots) != 1 || len(plan.Slots[0].Requests) != 1 || len(plan.Slots[0].Requests[0]) != 512 {
		t.Fatalf("512-token plan: slots=%d requests=%d first=%d",
			len(plan.Slots), len(plan.Slots[0].Requests), len(plan.Slots[0].Requests[0]))
	}

	// 1024 tokens → exactly two requests of 512 on one connection.
	plan, err = BuildSubscriptionPlan(ids(1024), 1, 1024, 512)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Slots) != 1 || len(plan.Slots[0].Requests) != 2 {
		t.Fatalf("1024-token plan: slots=%d requests=%d", len(plan.Slots), len(plan.Slots[0].Requests))
	}
	for i, r := range plan.Slots[0].Requests {
		if len(r) != 512 {
			t.Fatalf("request %d has %d tokens, want 512", i, len(r))
		}
	}

	// Over-capacity (connection limit exceeded on a single slot) never starts.
	if _, err := BuildSubscriptionPlan(ids(1025), 1, 1024, 512); err == nil {
		t.Fatal("ING-CAP-001: 1025 tokens on one connection must be rejected")
	}

	// 3000 tokens over 3 slots → chunks 1024, 1024, 952 (plan §Executive Summary).
	plan, err = BuildSubscriptionPlan(ids(3000), 3, 1024, 512)
	if err != nil {
		t.Fatal(err)
	}
	got := []int{}
	for _, s := range plan.Slots {
		got = append(got, len(s.Tokens))
	}
	want := []int{1024, 1024, 952}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("3000-token chunking: got %v, want %v", got, want)
		}
	}
}

func ids(n int) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = int32(i + 1)
	}
	return out
}
