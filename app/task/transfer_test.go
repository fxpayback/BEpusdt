package task

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/v03413/bepusdt/app/model"
)

func TestAmountMatchModeRejectsUnderpayment(t *testing.T) {
	if amountMatchMode(decimal.RequireFromString("1.57"), "1.58", string(model.UsdtTrc20), model.Classic) {
		t.Fatal("underpayment must not match the displayed order amount")
	}
}

func TestAmountMatchModeAcceptsExactPayment(t *testing.T) {
	if !amountMatchMode(decimal.RequireFromString("1.58"), "1.58", string(model.UsdtTrc20), model.Classic) {
		t.Fatal("exact payment should match in classic mode")
	}
}
