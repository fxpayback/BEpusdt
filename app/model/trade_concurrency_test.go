package model

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func TestBuildPendingOrderConcurrentDuplicateIsIdempotent(t *testing.T) {
	db := newTradeConcurrencyTestDB(t)
	params := OrderParams{
		Money:         decimal.RequireFromString("15"),
		ApiType:       OrderApiTypeEpusdtOrder,
		OrderId:       "tenant_order_duplicate",
		Fiat:          CNY,
		CurrencyLimit: "USDT,USDC",
	}

	const workers = 12
	start := make(chan struct{})
	tradeIDs := make(chan string, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			order, err := BuildPendingOrder(params)
			if err != nil {
				errs <- err
				return
			}
			tradeIDs <- order.TradeId
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	close(tradeIDs)

	for err := range errs {
		t.Fatalf("build pending order: %v", err)
	}
	var first string
	for tradeID := range tradeIDs {
		if first == "" {
			first = tradeID
		}
		if tradeID != first {
			t.Fatalf("duplicate merchant order returned different trade ids: %q and %q", first, tradeID)
		}
	}
	var count int64
	if err := db.Model(&Order{}).Where("order_id = ?", params.OrderId).Count(&count).Error; err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if count != 1 {
		t.Fatalf("created %d rows for one merchant order, want 1", count)
	}
}

func TestRebuildOrderConcurrentAllocationsRemainUnique(t *testing.T) {
	newTradeConcurrencyTestDB(t)
	first := mustBuildPendingOrder(t, "tenant_order_first")
	second := mustBuildPendingOrder(t, "tenant_order_second")

	start := make(chan struct{})
	results := make(chan Order, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i, order := range []Order{first, second} {
		wg.Add(1)
		go func(index int, pending Order) {
			defer wg.Done()
			<-start
			updated, err := RebuildOrder(pending, OrderParams{
				Money:             decimal.RequireFromString("15"),
				OrderId:           pending.OrderId,
				TradeType:         UsdtBep20,
				Fiat:              CNY,
				Timeout:           1200,
				ClientFingerprint: fmt.Sprintf("fingerprint-%d", index),
			})
			if err != nil {
				errs <- err
				return
			}
			results <- updated
		}(i, order)
	}
	close(start)
	wg.Wait()
	close(errs)
	close(results)

	for err := range errs {
		t.Fatalf("rebuild pending order: %v", err)
	}
	amounts := make([]string, 0, 2)
	for order := range results {
		amounts = append(amounts, order.Amount)
	}
	sort.Strings(amounts)
	want := []string{"2.5", "2.51"}
	if len(amounts) != len(want) || amounts[0] != want[0] || amounts[1] != want[1] {
		t.Fatalf("allocated amounts = %v, want %v", amounts, want)
	}
}

func TestRebuildOrderConcurrentSameOrderDoesNotCollideWithItself(t *testing.T) {
	newTradeConcurrencyTestDB(t)
	pending := mustBuildPendingOrder(t, "tenant_order_same")
	params := OrderParams{
		Money:             decimal.RequireFromString("15"),
		OrderId:           pending.OrderId,
		TradeType:         UsdtBep20,
		Fiat:              CNY,
		Timeout:           1200,
		ClientFingerprint: "same-browser",
	}

	start := make(chan struct{})
	results := make(chan Order, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			updated, err := RebuildOrder(pending, params)
			if err != nil {
				errs <- err
				return
			}
			results <- updated
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	close(results)

	for err := range errs {
		t.Fatalf("rebuild same pending order: %v", err)
	}
	for order := range results {
		if order.Amount != "2.5" {
			t.Fatalf("same order amount = %s, want 2.5", order.Amount)
		}
	}

	var stored Order
	if err := Db.Where("id = ?", pending.ID).Take(&stored).Error; err != nil {
		t.Fatalf("reload same order: %v", err)
	}
	if stored.Amount != "2.5" || stored.ClientFingerprint != "same-browser" {
		t.Fatalf("stored order amount/fingerprint = %q/%q", stored.Amount, stored.ClientFingerprint)
	}
}

func mustBuildPendingOrder(t *testing.T, orderID string) Order {
	t.Helper()
	order, err := BuildPendingOrder(OrderParams{
		Money:         decimal.RequireFromString("15"),
		ApiType:       OrderApiTypeEpusdtOrder,
		OrderId:       orderID,
		Fiat:          CNY,
		CurrencyLimit: "USDT,USDC",
	})
	if err != nil {
		t.Fatalf("build pending order %s: %v", orderID, err)
	}
	return order
}

func newTradeConcurrencyTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "trade-concurrency.db")
	db, err := gorm.Open(sqlite.Open(dbPath+"?cache=shared&mode=rwc&_pragma=busy_timeout(8000)"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := db.AutoMigrate(&Conf{}, &Wallet{}, &Order{}, &Rate{}); err != nil {
		t.Fatalf("migrate test db: %v", err)
	}

	now := time.Now().UTC()
	createdAt := Datetime(now)
	updatedAt := Datetime(now)
	configs := []Conf{
		{K: PaymentMinAmount, V: "0.01"},
		{K: PaymentMaxAmount, V: "99999"},
		{K: PaymentTimeout, V: "1200"},
		{K: RateSyncMaxAge, V: "900"},
		{K: AtomUSDT, V: "0.01"},
	}
	if err := db.Create(&configs).Error; err != nil {
		t.Fatalf("seed config: %v", err)
	}
	wallet := Wallet{
		Name:      "BSC",
		Status:    WaStatusEnable,
		Address:   "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		MatchAddr: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TradeType: string(UsdtBep20),
		AutoTimeAt: AutoTimeAt{
			CreatedAt: &createdAt,
			UpdatedAt: &updatedAt,
		},
	}
	if err := db.Create(&wallet).Error; err != nil {
		t.Fatalf("seed wallet: %v", err)
	}
	rate := Rate{
		Rate:    "6",
		Fiat:    string(CNY),
		Crypto:  string(USDT),
		RawRate: 6,
		AutoTimeAt: AutoTimeAt{
			CreatedAt: &createdAt,
			UpdatedAt: &updatedAt,
		},
	}

	previousDB := Db
	Db = db
	RefreshC()
	if err := db.Create(&rate).Error; err != nil {
		t.Fatalf("seed rate: %v", err)
	}
	t.Cleanup(func() {
		Db = previousDB
		if previousDB != nil {
			RefreshC()
		}
		sqlDB, dbErr := db.DB()
		if dbErr == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}
