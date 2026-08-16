package model

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestUniqueSuffixSequenceCoversEveryNonZeroSuffixOnce(t *testing.T) {
	sequence := uniqueSuffixSequence(999)
	if len(sequence) != 999 {
		t.Fatalf("suffix count = %d, want 999", len(sequence))
	}
	seen := make(map[int64]struct{}, len(sequence))
	for _, suffix := range sequence {
		if suffix < 1 || suffix > 999 {
			t.Fatalf("suffix %d outside 1..999", suffix)
		}
		if _, exists := seen[suffix]; exists {
			t.Fatalf("suffix %d repeated", suffix)
		}
		seen[suffix] = struct{}{}
	}
}

func TestAllocateUniqueTradeAmountPreservesBaseAndUsesExactFiveDecimals(t *testing.T) {
	wallet := collisionTestWallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", UsdtBep20)
	lock := map[string]bool{paymentAllocationKey(wallet.GetMatchAddr(), UsdtBep20, "1.61001"): true}
	_, amount, err := allocateUniqueTradeAmount([]Wallet{wallet}, decimal.RequireFromString("1.61"), 2, UsdtBep20, lock)
	if err != nil {
		t.Fatalf("allocate unique amount: %v", err)
	}
	parsed, err := decimal.NewFromString(amount)
	if err != nil || !parsed.GreaterThan(decimal.RequireFromString("1.61")) || !parsed.LessThan(decimal.RequireFromString("1.62")) {
		t.Fatalf("amount %q is outside (1.61, 1.62)", amount)
	}
	if len(amount) != len("1.61000") {
		t.Fatalf("amount %q does not preserve five decimal places", amount)
	}
}

func TestAllocateUniqueTradeAmountExhaustion(t *testing.T) {
	wallet := collisionTestWallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", UsdtBep20)
	lock := make(map[string]bool, 999)
	base := decimal.RequireFromString("1.61")
	unit := decimal.New(1, -uniquePaymentPrecision)
	for suffix := int64(1); suffix <= 999; suffix++ {
		amount := base.Add(unit.Mul(decimal.NewFromInt(suffix))).StringFixed(uniquePaymentPrecision)
		lock[paymentAllocationKey(wallet.GetMatchAddr(), UsdtBep20, amount)] = true
	}
	if _, _, err := allocateUniqueTradeAmount([]Wallet{wallet}, base, 2, UsdtBep20, lock); err == nil {
		t.Fatal("expected suffix exhaustion error")
	}
}
