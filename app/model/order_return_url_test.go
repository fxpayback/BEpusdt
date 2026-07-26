package model

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestOrderStoresSignedResumeReturnURL(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "return-url.db")), &gorm.Config{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get test sql db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&Order{}); err != nil {
		t.Fatalf("migrate test db: %v", err)
	}

	now := time.Now().UTC()
	confirmedAt := time.Unix(0, 0)
	returnURL := "https://llm.twinight.co/payment/result?resume_token=" + strings.Repeat("a", 320)
	order := Order{
		OrderId: "tw_sub2_return_url_test", TradeId: "return-url-test",
		Fiat: CNY, Crypto: USDT, Money: "1.23", Amount: "0", Rate: "0",
		Status: OrderStatusWaiting, ApiType: OrderApiTypeEpusdtOrder, ReturnUrl: returnURL,
		ExpiredAt: now.Add(20 * time.Minute), ConfirmedAt: &confirmedAt,
	}
	if err := db.Create(&order).Error; err != nil {
		t.Fatalf("create order: %v", err)
	}
	var stored Order
	if err := db.First(&stored, order.ID).Error; err != nil {
		t.Fatalf("load order: %v", err)
	}
	if stored.ReturnUrl != returnURL {
		t.Fatalf("return URL was truncated: got %d bytes, want %d", len(stored.ReturnUrl), len(returnURL))
	}
}
