package task

import (
	"testing"
	"time"

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

func TestReceivableOrderStatusesIncludeCanceled(t *testing.T) {
	statuses := receivableOrderStatuses()
	found := false
	for _, status := range statuses {
		if status == model.OrderStatusCanceled {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("recent canceled orders must remain receivable for late scanner recovery")
	}
}

func TestCanceledOrderOnlyMatchesTransfersBeforeCancellation(t *testing.T) {
	createdAt := model.Datetime(time.Now().Add(-10 * time.Minute))
	canceledAt := model.Datetime(time.Now().Add(-5 * time.Minute))
	order := model.Order{
		TradeType:     model.UsdtBep20,
		Amount:        "1.6",
		Address:       "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		MatchAddress:  "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AddressLocked: true,
		Status:        model.OrderStatusCanceled,
		ExpiredAt:     time.Now().Add(10 * time.Minute),
		AutoTimeAt: model.AutoTimeAt{
			CreatedAt: &createdAt,
			UpdatedAt: &canceledAt,
		},
	}
	base := transfer{
		TradeType:   model.UsdtBep20,
		Amount:      decimal.RequireFromString("1.6"),
		RecvAddress: order.MatchAddress,
	}

	beforeCancel := base
	beforeCancel.Timestamp = canceledAt.Time().Add(-time.Second)
	if !orderTransferMatch(order, beforeCancel) {
		t.Fatal("payment mined before cancellation should recover the canceled order")
	}

	afterCancel := base
	afterCancel.Timestamp = canceledAt.Time().Add(time.Second)
	if orderTransferMatch(order, afterCancel) {
		t.Fatal("payment mined after cancellation must not be assigned to the canceled order")
	}
}

func TestReconciliationExclusionAndBoundedEligibility(t *testing.T) {
	now := time.Now()
	normalCutoff := now.Add(-3 * time.Hour)
	reconciliationCutoff := now.Add(-24 * time.Hour)
	excluded := reconciliationExcludedOrderIDs(" 2616, merchant-closed ;\n")

	if _, ok := excluded["2616"]; !ok {
		t.Fatal("expected manually completed order to be excluded")
	}
	if _, ok := excluded["merchant-closed"]; !ok {
		t.Fatal("expected semicolon-delimited exclusion")
	}

	base := model.Order{TradeType: model.UsdtBep20, Status: model.OrderStatusExpired, ExpiredAt: now.Add(-2 * time.Hour)}
	if !receivableOrderEligible(base, normalCutoff, reconciliationCutoff, excluded) {
		t.Fatal("expired BSC USDT order inside reconciliation window should be eligible")
	}

	base.OrderId = "2616"
	if receivableOrderEligible(base, normalCutoff, reconciliationCutoff, excluded) {
		t.Fatal("explicitly excluded order must not be eligible")
	}

	base.OrderId = ""
	base.ExpiredAt = now.Add(-25 * time.Hour)
	if receivableOrderEligible(base, normalCutoff, reconciliationCutoff, excluded) {
		t.Fatal("expired order outside bounded reconciliation window must not be eligible")
	}

	base.Status = model.OrderStatusCanceled
	base.ExpiredAt = now.Add(-4 * time.Hour)
	if receivableOrderEligible(base, normalCutoff, reconciliationCutoff, excluded) {
		t.Fatal("canceled orders must not enter the extended reconciliation window")
	}

	base.Status = model.OrderStatusExpired
	base.TradeType = model.UsdcBep20
	if receivableOrderEligible(base, normalCutoff, reconciliationCutoff, excluded) {
		t.Fatal("extended reconciliation must remain scoped to BSC USDT")
	}
}

func TestEarlierTimeUsesNormalCutoffWhenReconciliationDisabled(t *testing.T) {
	normal := time.Now().Add(-3 * time.Hour)
	if got := earlierTime(normal, time.Time{}); !got.Equal(normal) {
		t.Fatalf("disabled reconciliation cutoff = %v, want normal cutoff %v", got, normal)
	}
}
