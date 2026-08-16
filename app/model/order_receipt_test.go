package model

import (
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestMarkConfirmingReceiptIsUniqueAndIdempotent(t *testing.T) {
	db := newCollisionTestDB(t)
	if err := db.Exec("CREATE UNIQUE INDEX idx_bep_order_receipt_key_unique_test ON bep_order (ref_receipt_key) WHERE ref_receipt_key <> ''").Error; err != nil {
		t.Fatalf("create receipt uniqueness index: %v", err)
	}

	first := receiptTestOrder("merchant-1", "trade-1")
	second := receiptTestOrder("merchant-2", "trade-2")
	if err := db.Create(first).Error; err != nil {
		t.Fatalf("seed first order: %v", err)
	}
	if err := db.Create(second).Error; err != nil {
		t.Fatalf("seed second order: %v", err)
	}

	confirmedAt := time.Now().UTC()
	receiptKey := "bsc:0xhash:0x1"
	if err := first.MarkConfirmingReceipt(100, "0xsender", "0xhash", receiptKey, confirmedAt, decimal.RequireFromString("10")); err != nil {
		t.Fatalf("claim first receipt: %v", err)
	}
	if err := first.MarkConfirmingReceipt(100, "0xsender", "0xhash", receiptKey, confirmedAt, decimal.RequireFromString("10")); err != nil {
		t.Fatalf("idempotent receipt resubmission: %v", err)
	}

	err := second.MarkConfirmingReceipt(100, "0xsender", "0xhash", receiptKey, confirmedAt, decimal.RequireFromString("10"))
	if !errors.Is(err, ErrReceiptAlreadyClaimed) {
		t.Fatalf("duplicate claim error = %v, want %v", err, ErrReceiptAlreadyClaimed)
	}
	if second.Status != OrderStatusWaiting || second.RefReceiptKey != "" || second.RefHash != "" {
		t.Fatalf("failed claim mutated in-memory order: status=%d receipt=%q hash=%q", second.Status, second.RefReceiptKey, second.RefHash)
	}

	var stored Order
	if err := db.Where("id = ?", second.ID).First(&stored).Error; err != nil {
		t.Fatalf("reload duplicate order: %v", err)
	}
	if stored.Status != OrderStatusWaiting || stored.RefReceiptKey != "" || stored.RefHash != "" {
		t.Fatalf("failed claim mutated database order: status=%d receipt=%q hash=%q", stored.Status, stored.RefReceiptKey, stored.RefHash)
	}
}

func receiptTestOrder(orderID, tradeID string) *Order {
	now := time.Now().UTC()
	createdAt := Datetime(now)
	updatedAt := Datetime(now)
	confirmedAt := time.Time{}
	return &Order{
		OrderId: orderID, TradeId: tradeID, TradeType: UsdtBep20,
		Fiat: CNY, Crypto: USDT, Rate: "1", Amount: "10", Money: "10",
		Address: "0xrecipient", MatchAddress: "0xrecipient", Status: OrderStatusWaiting,
		ExpiredAt: now.Add(20 * time.Minute), ConfirmedAt: &confirmedAt,
		AutoTimeAt: AutoTimeAt{CreatedAt: &createdAt, UpdatedAt: &updatedAt},
	}
}
