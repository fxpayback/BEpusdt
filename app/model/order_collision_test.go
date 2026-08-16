package model

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func TestCalcTradeAmountOnlyIncrementsForExactActiveCollision(t *testing.T) {
	tests := []struct {
		name          string
		candidate     Wallet
		tradeType     TradeType
		money         string
		existingOrder *Order
		wantBase      string
	}{
		{
			name:      "no collision",
			candidate: collisionTestWallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", UsdtBep20),
			tradeType: UsdtBep20,
			money:     "15.00",
			wantBase:  "2.50",
		},
		{
			name:      "same wallet route and amount",
			candidate: collisionTestWallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", UsdtBep20),
			tradeType: UsdtBep20,
			money:     "15.00",
			existingOrder: collisionTestOrder(
				"0xDISPLAYADDRESS000000000000000000000001",
				"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				UsdtBep20,
				"2.5",
			),
			wantBase: "2.50",
		},
		{
			name:      "different wallet",
			candidate: collisionTestWallet("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", UsdtBep20),
			tradeType: UsdtBep20,
			money:     "15.00",
			existingOrder: collisionTestOrder(
				"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				UsdtBep20,
				"2.5",
			),
			wantBase: "2.50",
		},
		{
			name:      "different route",
			candidate: collisionTestWallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", UsdtPolygon),
			tradeType: UsdtPolygon,
			money:     "15.00",
			existingOrder: collisionTestOrder(
				"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				UsdtBep20,
				"2.5",
			),
			wantBase: "2.50",
		},
		{
			name:      "different amount",
			candidate: collisionTestWallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", UsdtBep20),
			tradeType: UsdtBep20,
			money:     "15.06",
			existingOrder: collisionTestOrder(
				"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				UsdtBep20,
				"2.5",
			),
			wantBase: "2.51",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := newCollisionTestDB(t)
			if test.existingOrder != nil {
				if err := db.Create(test.existingOrder).Error; err != nil {
					t.Fatalf("seed pending order: %v", err)
				}
			}

			_, got, err := CalcTradeAmount([]Wallet{test.candidate}, decimal.RequireFromString("6"), OrderParams{
				Money:     decimal.RequireFromString(test.money),
				TradeType: test.tradeType,
			})
			if err != nil {
				t.Fatalf("calculate trade amount: %v", err)
			}
			if test.tradeType == UsdtBep20 || test.tradeType == UsdtPolygon {
				base := decimal.RequireFromString(test.wantBase)
				parsed, parseErr := decimal.NewFromString(got)
				upper := base.Add(decimal.RequireFromString("0.01"))
				if parseErr != nil || !parsed.GreaterThan(base) || !parsed.LessThan(upper) || len(got) != len("2.50000") {
					t.Fatalf("unexpected unique amount: got %s base %s", got, base.String())
				}
			} else if got != test.wantBase {
				t.Fatalf("unexpected amount: got %s want %s", got, test.wantBase)
			}
		})
	}
}

func newCollisionTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "order-collision.db")
	db, err := gorm.Open(sqlite.Open(dbPath+"?cache=shared&mode=rwc"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := db.AutoMigrate(&Conf{}, &Order{}); err != nil {
		t.Fatalf("migrate test db: %v", err)
	}
	if err := db.Create(&[]Conf{
		{K: AtomUSDT, V: "0.01"},
		{K: PaymentUniqueAmountTypes, V: "usdt.bep20,usdc.bep20,usdt.polygon,usdc.polygon"},
	}).Error; err != nil {
		t.Fatalf("seed atomicity: %v", err)
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
	return db
}

func collisionTestWallet(address string, tradeType TradeType) Wallet {
	return Wallet{Address: address, MatchAddr: address, TradeType: string(tradeType)}
}

func collisionTestOrder(address, matchAddress string, tradeType TradeType, amount string) *Order {
	now := time.Now().UTC()
	createdAt := Datetime(now)
	updatedAt := Datetime(now)
	confirmedAt := now
	return &Order{
		OrderId:      "merchant-order-1",
		TradeId:      "bepusdt-trade-1",
		TradeType:    tradeType,
		Fiat:         CNY,
		Crypto:       USDT,
		Rate:         "6",
		Amount:       amount,
		Money:        "15",
		Address:      address,
		MatchAddress: matchAddress,
		Status:       OrderStatusWaiting,
		ExpiredAt:    now.Add(20 * time.Minute),
		ConfirmedAt:  &confirmedAt,
		AutoTimeAt: AutoTimeAt{
			CreatedAt: &createdAt,
			UpdatedAt: &updatedAt,
		},
	}
}
