package epusdt

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/utils"
	"gorm.io/gorm"
)

func newQueryTransactionTestRouter(t *testing.T, order model.Order, authToken string) *gin.Engine {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "query-transaction.db")
	db, err := gorm.Open(sqlite.Open(dbPath+"?cache=shared&mode=rwc"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := db.AutoMigrate(&model.Conf{}, &model.Order{}); err != nil {
		t.Fatalf("migrate test db: %v", err)
	}
	if err := db.Create(&model.Conf{K: model.ApiAuthToken, V: authToken}).Error; err != nil {
		t.Fatalf("seed auth token: %v", err)
	}
	if err := db.Create(&order).Error; err != nil {
		t.Fatalf("seed order: %v", err)
	}

	previousDB := model.Db
	model.Db = db
	model.RefreshC()
	t.Cleanup(func() {
		model.Db = previousDB
		sqlDB, dbErr := db.DB()
		if dbErr == nil {
			_ = sqlDB.Close()
		}
	})

	gin.SetMode(gin.TestMode)
	router := gin.New()
	h := new(Epusdt)
	router.POST("/query", h.SignVerify, h.QueryTransaction)
	return router
}

func queryTransactionTestOrder() model.Order {
	now := time.Now().UTC().Truncate(time.Second)
	createdAt := model.Datetime(now.Add(-time.Minute))
	updatedAt := model.Datetime(now)
	confirmedAt := now.Add(-30 * time.Second)
	return model.Order{
		OrderId:      "tenant_sub2_order_123",
		TradeId:      "bepusdt-trade-123",
		TradeType:    model.UsdtTrc20,
		Fiat:         model.CNY,
		Crypto:       model.USDT,
		Rate:         "6.20",
		Amount:       "2.4194",
		Money:        "15.00",
		Address:      "TTestAddress123456789012345678901",
		MatchAddress: "TTestAddress123456789012345678901",
		Status:       model.OrderStatusSuccess,
		ApiType:      model.OrderApiTypeEpusdtOrder,
		RefHash:      "blockchain-transaction-hash",
		ExpiredAt:    now.Add(19 * time.Minute),
		ConfirmedAt:  &confirmedAt,
		AutoTimeAt: model.AutoTimeAt{
			CreatedAt: &createdAt,
			UpdatedAt: &updatedAt,
		},
	}
}

func TestQueryTransactionUsesSignedMerchantPathWithoutFingerprint(t *testing.T) {
	const authToken = "merchant-auth-token"
	order := queryTransactionTestOrder()
	order.ClientFingerprint = "browser-only-fingerprint"
	router := newQueryTransactionTestRouter(t, order, authToken)

	payload := map[string]any{"trade_id": order.TradeId}
	payload["signature"] = utils.EpusdtSign(payload, authToken)
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	var got struct {
		StatusCode int `json:"status_code"`
		Data       struct {
			TradeID            string `json:"trade_id"`
			OrderID            string `json:"order_id"`
			TradeType          string `json:"trade_type"`
			Fiat               string `json:"fiat"`
			Currency           string `json:"currency"`
			Amount             string `json:"amount"`
			Status             int    `json:"status"`
			BlockTransactionID string `json:"block_transaction_id"`
			ConfirmedAt        string `json:"confirmed_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.StatusCode != http.StatusOK {
		t.Fatalf("unexpected response: %s", resp.Body.String())
	}
	if got.Data.TradeID != order.TradeId || got.Data.OrderID != order.OrderId {
		t.Fatalf("unexpected identifiers: %+v", got.Data)
	}
	if got.Data.TradeType != string(order.TradeType) || got.Data.Fiat != string(order.Fiat) || got.Data.Currency != string(order.Crypto) {
		t.Fatalf("unexpected settlement metadata: %+v", got.Data)
	}
	if got.Data.Amount != order.Money || got.Data.Status != model.OrderStatusSuccess {
		t.Fatalf("unexpected payment state: %+v", got.Data)
	}
	if got.Data.BlockTransactionID != order.RefHash || got.Data.ConfirmedAt == "" {
		t.Fatalf("missing settlement identity: %+v", got.Data)
	}
}

func TestQueryTransactionRejectsInvalidSignature(t *testing.T) {
	order := queryTransactionTestOrder()
	router := newQueryTransactionTestRouter(t, order, "merchant-auth-token")

	body := []byte(`{"trade_id":"bepusdt-trade-123","signature":"invalid"}`)
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	var got struct {
		StatusCode int    `json:"status_code"`
		Message    string `json:"message"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.StatusCode != http.StatusBadRequest || got.Message != "签名错误" {
		t.Fatalf("expected signature rejection, got %s", resp.Body.String())
	}
}

func TestQueryTransactionRejectsAnotherMerchantToken(t *testing.T) {
	order := queryTransactionTestOrder()
	router := newQueryTransactionTestRouter(t, order, "legacy-token")
	model.SetK(model.ApiMerchantTokens, `{"tenant_sub2_":"owner-token","other_":"other-token"}`)

	payload := map[string]any{"trade_id": order.TradeId}
	payload["signature"] = utils.EpusdtSign(payload, "other-token")
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	var got struct {
		StatusCode int    `json:"status_code"`
		Message    string `json:"message"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.StatusCode != http.StatusBadRequest || got.Message != "签名错误" {
		t.Fatalf("expected cross-merchant signature rejection, got %s", resp.Body.String())
	}
}

func TestSignVerifyUsesMerchantPrefixForOrderCreation(t *testing.T) {
	order := queryTransactionTestOrder()
	router := newQueryTransactionTestRouter(t, order, "legacy-token")
	model.SetK(model.ApiMerchantTokens, `{"tenant_sub2_":"owner-token","other_":"other-token"}`)
	h := new(Epusdt)
	router.POST("/create", h.SignVerify, func(ctx *gin.Context) { ctx.Status(http.StatusNoContent) })

	for name, testCase := range map[string]struct {
		token      string
		wantStatus int
	}{
		"owner":   {token: "owner-token", wantStatus: http.StatusNoContent},
		"foreign": {token: "other-token", wantStatus: http.StatusOK},
	} {
		t.Run(name, func(t *testing.T) {
			payload := map[string]any{"order_id": "tenant_sub2_new_order"}
			payload["signature"] = utils.EpusdtSign(payload, testCase.token)
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			router.ServeHTTP(resp, req)
			if resp.Code != testCase.wantStatus {
				t.Fatalf("unexpected status %d: %s", resp.Code, resp.Body.String())
			}
		})
	}
}
