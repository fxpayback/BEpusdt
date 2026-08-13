package task

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/v03413/bepusdt/app/log"

	"github.com/tidwall/gjson"
	"github.com/v03413/bepusdt/app/conf"
)

func TestEventTransferRequestFiltersBSCContracts(t *testing.T) {
	e := evm{Network: conf.Bsc}
	body, err := e.eventTransferRequest(evmBlock{From: 10, To: 19})
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	request := gjson.ParseBytes(body)
	if request.Get("method").String() != "eth_getLogs" {
		t.Fatalf("unexpected method: %s", request.Get("method").String())
	}
	if request.Get("params.0.fromBlock").String() != "0xa" || request.Get("params.0.toBlock").String() != "0x13" {
		t.Fatalf("unexpected block range: %s", string(body))
	}
	addresses := request.Get("params.0.address").Array()
	if len(addresses) != 2 {
		t.Fatalf("expected BSC USDT and USDC contract filters, got %d", len(addresses))
	}
	if request.Get("params.0.topics.0").String() != evmTransferEvent {
		t.Fatalf("unexpected event topic: %s", string(body))
	}
}

func TestSplitRPCEndpointsPreservesOrderAndDeduplicates(t *testing.T) {
	got := splitRPCEndpoints(" https://primary.example/rpc, https://fallback.example/rpc;https://primary.example/rpc", "https://last.example/rpc")
	want := []string{"https://primary.example/rpc", "https://fallback.example/rpc", "https://last.example/rpc"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("endpoint %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestValidateRPCResponseStrictShapes(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		kind    rpcResultKind
		wantErr bool
	}{
		{name: "malformed", body: "{", kind: rpcResultAny, wantErr: true},
		{name: "json rpc error", body: "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32000}}", kind: rpcResultAny, wantErr: true},
		{name: "missing result", body: "{\"jsonrpc\":\"2.0\",\"id\":1}", kind: rpcResultAny, wantErr: true},
		{name: "null array", body: "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":null}", kind: rpcResultArray, wantErr: true},
		{name: "wrong array shape", body: "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}", kind: rpcResultArray, wantErr: true},
		{name: "allow null receipt", body: "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":null}", kind: rpcResultAllowNull},
		{name: "invalid batch item", body: "[{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":null}]", kind: rpcResultBatchObjects, wantErr: true},
		{name: "valid batch", body: "[{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"number\":\"0x1\",\"timestamp\":\"0x2\",\"transactions\":[]}}]", kind: rpcResultBatchObjects},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRPCResponse([]byte(tt.body), tt.kind)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateRPCResponse error = %v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func TestRPCRequestFailsOverAndRetainsSuccessfulEndpoint(t *testing.T) {
	if err := log.Init(t.TempDir()); err != nil {
		t.Fatalf("initialize test logger: %v", err)
	}
	defer log.Close()

	var primaryCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls.Add(1)
		http.Error(w, "temporary failure", http.StatusBadGateway)
	}))
	defer primary.Close()

	var fallbackCalls atomic.Int32
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":[]}"))
	}))
	defer fallback.Close()

	e := &evm{Network: conf.Bsc, Client: fallback.Client(), RPCEndpoints: []string{primary.URL, fallback.URL}}
	payload := []byte("{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[],\"id\":1}")
	if _, err := e.rpcRequest(context.Background(), "eth_getLogs", "1-2", payload, rpcResultArray); err != nil {
		t.Fatalf("first request failed after failover: %v", err)
	}
	if got := primaryCalls.Load(); got != rpcAttemptsPerEndpoint {
		t.Fatalf("primary calls = %d, want %d", got, rpcAttemptsPerEndpoint)
	}
	if got := fallbackCalls.Load(); got != 1 {
		t.Fatalf("fallback calls = %d, want 1", got)
	}

	if _, err := e.rpcRequest(context.Background(), "eth_getLogs", "3-4", payload, rpcResultArray); err != nil {
		t.Fatalf("second request failed: %v", err)
	}
	if got := primaryCalls.Load(); got != rpcAttemptsPerEndpoint {
		t.Fatalf("primary was retried after successful endpoint retention: %d calls", got)
	}
	if got := fallbackCalls.Load(); got != 2 {
		t.Fatalf("fallback calls after retention = %d, want 2", got)
	}
}

func TestChunkEVMBlockRangeIsExact(t *testing.T) {
	got := chunkEVMBlockRange(101, 125, 10)
	want := []evmBlock{{From: 101, To: 110}, {From: 111, To: 120}, {From: 121, To: 125}}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("range %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
