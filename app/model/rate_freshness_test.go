package model

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func TestGetOrderRateRejectsStaleRatesAndAppliesMultiplier(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rate-freshness.db")
	db, err := gorm.Open(sqlite.Open(dbPath+"?cache=shared&mode=rwc"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := db.AutoMigrate(&Conf{}, &Rate{}); err != nil {
		t.Fatalf("migrate test db: %v", err)
	}
	configs := []Conf{
		{K: RateSyncMaxAge, V: "900"},
		{K: ConfKey("rate_float_USDC_CNY"), V: "~0.93"},
	}
	if err := db.Create(&configs).Error; err != nil {
		t.Fatalf("seed config: %v", err)
	}

	previousDB := Db
	Db = db
	RefreshC()
	t.Cleanup(func() {
		Db = previousDB
		sqlDB, dbErr := db.DB()
		if dbErr == nil {
			_ = sqlDB.Close()
		}
	})

	staleTime := Datetime(time.Now().Add(-16 * time.Minute))
	if err := db.Create(&Rate{
		Rate: "7.00", Fiat: string(CNY), Crypto: string(USDT), RawRate: 7,
		AutoTimeAt: AutoTimeAt{CreatedAt: &staleTime, UpdatedAt: &staleTime},
	}).Error; err != nil {
		t.Fatalf("seed stale rate: %v", err)
	}
	if _, err := GetOrderRate(USDT, CNY, ""); err == nil {
		t.Fatal("expected stale rate to be rejected")
	}

	freshTime := Datetime(time.Now().Add(-time.Minute))
	if err := db.Create(&Rate{
		Rate: "6.231", Fiat: string(CNY), Crypto: string(USDC), RawRate: 6.7,
		AutoTimeAt: AutoTimeAt{CreatedAt: &freshTime, UpdatedAt: &freshTime},
	}).Error; err != nil {
		t.Fatalf("seed fresh rate: %v", err)
	}
	got, err := GetOrderRate(USDC, CNY, "~0.93")
	if err != nil {
		t.Fatalf("get fresh rate: %v", err)
	}
	want := decimal.RequireFromString("6.231")
	if !got.Equal(want) {
		t.Fatalf("unexpected adjusted rate: got %s want %s", got, want)
	}
}

func TestGetOrderRateRejectsFutureAndNonPositiveRates(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rate-validation.db")
	db, err := gorm.Open(sqlite.Open(dbPath+"?cache=shared&mode=rwc"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := db.AutoMigrate(&Conf{}, &Rate{}); err != nil {
		t.Fatalf("migrate test db: %v", err)
	}
	configs := []Conf{
		{K: RateSyncMaxAge, V: "900"},
		{K: ConfKey("rate_float_USDC_CNY"), V: "~0"},
	}
	if err := db.Create(&configs).Error; err != nil {
		t.Fatalf("seed config: %v", err)
	}

	previousDB := Db
	Db = db
	RefreshC()
	t.Cleanup(func() {
		Db = previousDB
		sqlDB, dbErr := db.DB()
		if dbErr == nil {
			_ = sqlDB.Close()
		}
	})

	futureTime := Datetime(time.Now().Add(2 * time.Minute))
	if err := db.Create(&Rate{
		Rate: "7.00", Fiat: string(CNY), Crypto: string(USDT), RawRate: 7,
		AutoTimeAt: AutoTimeAt{CreatedAt: &futureTime, UpdatedAt: &futureTime},
	}).Error; err != nil {
		t.Fatalf("seed future rate: %v", err)
	}
	if _, err := GetOrderRate(USDT, CNY, ""); err == nil {
		t.Fatal("expected future-dated rate to be rejected")
	}

	freshTime := Datetime(time.Now().Add(-time.Minute))
	if err := db.Create(&Rate{
		Rate: "6.70", Fiat: string(CNY), Crypto: string(USDC), RawRate: 6.7,
		AutoTimeAt: AutoTimeAt{CreatedAt: &freshTime, UpdatedAt: &freshTime},
	}).Error; err != nil {
		t.Fatalf("seed fresh rate: %v", err)
	}
	if _, err := GetOrderRate(USDC, CNY, "~0"); err == nil {
		t.Fatal("expected non-positive adjusted rate to be rejected")
	}
}
