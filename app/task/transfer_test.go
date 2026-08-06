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
