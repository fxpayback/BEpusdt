package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
)

const (
	testEVMHash      = "0x1111111111111111111111111111111111111111111111111111111111111111"
	testEVMFrom      = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testEVMRecipient = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type submittedRPCFixture struct {
	chainID       string
	receiptStatus string
	contract      string
	recipient     string
	amount        string
	timestamp     int64
	head          string
	nullReceipt   bool
	ambiguous     bool
	httpStatus    int
}

func TestInspectSubmittedTransactionValidation(t *testing.T) {
	created := time.Unix(1_700_000_000, 0).UTC()
	tests := []struct {
		name      string
		mutate    func(*submittedRPCFixture)
		wantState SubmittedTransactionState
		wantErr   error
	}{
		{name: "exact confirmed transfer", wantState: SubmittedTransactionConfirmed},
		{name: "insufficient confirmations", mutate: func(f *submittedRPCFixture) { f.head = "0x6e" }, wantState: SubmittedTransactionConfirming},
		{name: "wrong chain id", mutate: func(f *submittedRPCFixture) { f.chainID = "0x89" }, wantState: SubmittedTransactionPending, wantErr: ErrSubmittedTransactionInvalid},
		{name: "wrong token contract", mutate: func(f *submittedRPCFixture) { f.contract = conf.UsdcBep20 }, wantState: SubmittedTransactionPending, wantErr: ErrSubmittedTransactionInvalid},
		{name: "wrong recipient", mutate: func(f *submittedRPCFixture) { f.recipient = testEVMFrom }, wantState: SubmittedTransactionPending, wantErr: ErrSubmittedTransactionInvalid},
		{name: "wrong amount", mutate: func(f *submittedRPCFixture) { f.amount = evmTestAmount(11, 18) }, wantState: SubmittedTransactionPending, wantErr: ErrSubmittedTransactionInvalid},
		{name: "transfer before order", mutate: func(f *submittedRPCFixture) { f.timestamp = created.Add(-time.Second).Unix() }, wantState: SubmittedTransactionPending, wantErr: ErrSubmittedTransactionInvalid},
		{name: "transfer at expiry", mutate: func(f *submittedRPCFixture) { f.timestamp = created.Add(10 * time.Minute).Unix() }, wantState: SubmittedTransactionPending, wantErr: ErrSubmittedTransactionInvalid},
		{name: "reverted receipt", mutate: func(f *submittedRPCFixture) { f.receiptStatus = "0x0" }, wantState: SubmittedTransactionPending, wantErr: ErrSubmittedTransactionInvalid},
		{name: "missing receipt", mutate: func(f *submittedRPCFixture) { f.nullReceipt = true }, wantState: SubmittedTransactionPending},
		{name: "ambiguous matching logs", mutate: func(f *submittedRPCFixture) { f.ambiguous = true }, wantState: SubmittedTransactionPending, wantErr: ErrSubmittedTransactionInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := defaultSubmittedRPCFixture(created)
			if tt.mutate != nil {
				tt.mutate(&fixture)
			}
			server := newSubmittedRPCServer(t, fixture)
			defer server.Close()

			verifier := &evm{
				Network:      conf.Bsc,
				Block:        block{ConfirmedOffset: 15},
				Client:       server.Client(),
				RPCEndpoints: []string{server.URL},
			}
			result, err := verifier.inspectSubmittedTransaction(context.Background(), evmTestOrder(created), testEVMHash)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if result.State != tt.wantState {
				t.Fatalf("state = %s, want %s", result.State, tt.wantState)
			}
			if tt.name == "exact confirmed transfer" {
				if result.ReceiptKey != "bsc:"+testEVMHash+":0x1" {
					t.Fatalf("receipt key = %q", result.ReceiptKey)
				}
				if result.Confirmations != 15 {
					t.Fatalf("confirmations = %d, want 15", result.Confirmations)
				}
			}
		})
	}
}

func TestInspectSubmittedTransactionProviderErrorsStayPending(t *testing.T) {
	if err := log.Init(t.TempDir()); err != nil {
		t.Fatalf("initialize test logger: %v", err)
	}
	defer log.Close()

	for _, status := range []int{http.StatusTooManyRequests, http.StatusForbidden} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			fixture := defaultSubmittedRPCFixture(time.Unix(1_700_000_000, 0).UTC())
			fixture.httpStatus = status
			server := newSubmittedRPCServer(t, fixture)
			defer server.Close()

			verifier := &evm{Network: conf.Bsc, Block: block{ConfirmedOffset: 15}, Client: server.Client(), RPCEndpoints: []string{server.URL}}
			result, err := verifier.inspectSubmittedTransaction(context.Background(), evmTestOrder(time.Unix(1_700_000_000, 0).UTC()), testEVMHash)
			if err != nil {
				t.Fatalf("provider error escaped verifier: %v", err)
			}
			if result.State != SubmittedTransactionPending || result.ReceiptKey != "" || result.Transfer.BlockNum != 0 {
				t.Fatalf("provider failure created settlement evidence: %+v", result)
			}
		})
	}
}

func TestCanonicalEVMLogIndex(t *testing.T) {
	for raw, want := range map[string]string{"0x0": "0x0", "0x01": "0x1", "0xA": "0xa"} {
		got, ok := canonicalEVMLogIndex(raw)
		if !ok || got != want {
			t.Fatalf("canonicalEVMLogIndex(%q) = %q, %v; want %q", raw, got, ok, want)
		}
	}
	for _, raw := range []string{"", "1", "0x", "0xzz"} {
		if got, ok := canonicalEVMLogIndex(raw); ok {
			t.Fatalf("canonicalEVMLogIndex(%q) unexpectedly accepted %q", raw, got)
		}
	}
}

func defaultSubmittedRPCFixture(created time.Time) submittedRPCFixture {
	return submittedRPCFixture{
		chainID:       "0x38",
		receiptStatus: "0x1",
		contract:      conf.UsdtBep20,
		recipient:     testEVMRecipient,
		amount:        evmTestAmount(10, 18),
		timestamp:     created.Add(time.Minute).Unix(),
		head:          "0x73",
	}
}

func evmTestOrder(created time.Time) model.Order {
	createdAt := model.Datetime(created)
	return model.Order{
		TradeType:    model.UsdtBep20,
		Amount:       "10",
		Address:      testEVMRecipient,
		MatchAddress: testEVMRecipient,
		Status:       model.OrderStatusWaiting,
		ExpiredAt:    created.Add(10 * time.Minute),
		AutoTimeAt:   model.AutoTimeAt{CreatedAt: &createdAt},
	}
}

func evmTestAmount(amount int64, decimals int) string {
	value := new(big.Int).Mul(big.NewInt(amount), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	return "0x" + fmt.Sprintf("%064x", value)
}

func evmTestTopic(address string) string {
	return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(strings.ToLower(address), "0x")
}

func newSubmittedRPCServer(t *testing.T, fixture submittedRPCFixture) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fixture.httpStatus != 0 {
			http.Error(w, "provider unavailable", fixture.httpStatus)
			return
		}
		var request struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		var result any
		switch request.Method {
		case "eth_chainId":
			result = fixture.chainID
		case "eth_getTransactionReceipt":
			if fixture.nullReceipt {
				result = nil
				break
			}
			logs := []map[string]any{evmTestLog(fixture.contract, fixture.recipient, fixture.amount, "0x01")}
			if fixture.ambiguous {
				logs = append(logs, evmTestLog(fixture.contract, fixture.recipient, fixture.amount, "0x2"))
			}
			result = map[string]any{
				"status": fixture.receiptStatus, "transactionHash": testEVMHash,
				"blockNumber": "0x64", "logs": logs,
			}
		case "eth_getBlockByNumber":
			result = map[string]any{"timestamp": fmt.Sprintf("0x%x", fixture.timestamp)}
		case "eth_blockNumber":
			result = fixture.head
		default:
			http.Error(w, "unexpected method", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	}))
}

func evmTestLog(contract, recipient, amount, logIndex string) map[string]any {
	return map[string]any{
		"address":  contract,
		"topics":   []string{evmTransferEvent, evmTestTopic(testEVMFrom), evmTestTopic(recipient)},
		"data":     amount,
		"logIndex": logIndex,
	}
}
