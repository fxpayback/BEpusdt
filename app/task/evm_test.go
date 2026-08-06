package task

import (
	"testing"

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
