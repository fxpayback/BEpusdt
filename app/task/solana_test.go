package task

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
)

const solanaAffectedSlot = 438736944

const solanaAffectedBlock = `{
  "jsonrpc":"2.0",
  "result":{
    "blockTime":1786506275,
    "transactions":[{
      "meta":{
        "loadedAddresses":{"readonly":[],"writable":[]},
        "innerInstructions":[{
          "index":3,
          "instructions":[
            {"accounts":[8],"data":"84eT","programIdIndex":10},
            {"accounts":[0,1],"data":"11119os1e9qSs2u7TsThXqkBSRVFxhmYaFKFZ1waB2X7armDmvK3p5GmLdUxYdg3h7QSrL","programIdIndex":3},
            {"accounts":[1],"data":"P","programIdIndex":10},
            {"accounts":[1,8],"data":"6ZEYwwVAxLPYhd9YKLKxMj93uQ4b8taPTcFnCUc1PL2DW","programIdIndex":10}
          ]
        }],
        "preTokenBalances":[{
          "accountIndex":2,
          "mint":"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
          "owner":"EDCqa2hCeToyBWJMGzJV1d5Qcez2HMHoYdzSS4tnvBtz",
          "programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        }],
        "postTokenBalances":[
          {
            "accountIndex":1,
            "mint":"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
            "owner":"D6rH1HeA3YGsMfSoFAm2FAcWuB1KUq7kuLcLz2Ka2snY",
            "programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
          },
          {
            "accountIndex":2,
            "mint":"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
            "owner":"EDCqa2hCeToyBWJMGzJV1d5Qcez2HMHoYdzSS4tnvBtz",
            "programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
          }
        ]
      },
      "transaction":{
        "message":{
          "accountKeys":[
            "EDCqa2hCeToyBWJMGzJV1d5Qcez2HMHoYdzSS4tnvBtz",
            "Fc9Cxen24mdXYEHvWVzkyBGQDxv6LfLbZCcKuiTc6maL",
            "pJjXeYaLxqD6iK6CvvzGM5RZM5yaJZnzv96BBwSeeUa",
            "11111111111111111111111111111111",
            "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
            "ComputeBudget111111111111111111111111111111",
            "D6rH1HeA3YGsMfSoFAm2FAcWuB1KUq7kuLcLz2Ka2snY",
            "DeJBGdMFa1uynnnKiwrVioatTuHmNLpyFKnmB5kaFdzQ",
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
            "SysvarRent111111111111111111111111111111111",
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
          ],
          "instructions":[
            {"accounts":[],"data":"3rVnuUEJgZLj","programIdIndex":5},
            {"accounts":[],"data":"FRcYz3","programIdIndex":5},
            {"accounts":[6],"data":"11111111111111111111111111111111","programIdIndex":7},
            {"accounts":[0,1,6,8,3,10,9],"data":"","programIdIndex":4},
            {"accounts":[2,8,1,0,0],"data":"g7FhmVGJNk5zd","programIdIndex":10}
          ]
        },
        "signatures":["5CM3S19FtpxfFQsQmZNwcadm46MMEqr2Zb9jH7UFEv9edbKGboFtrihKxBQuvxqWRm7p78dJpXxXpHR5PWTpDykL"]
      }
    }]
  }
}`

type solanaRoundTripFunc func(*http.Request) (*http.Response, error)

func (f solanaRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func solanaClientWithBody(status int, body string) *http.Client {
	return &http.Client{Transport: solanaRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: status,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}
}

func TestParseSolanaBlockAffectedTransferCheckedWithNewDestinationAccount(t *testing.T) {
	s := newSolana()
	timestamp := time.Unix(1786506275, 0)
	batches, err := s.parseSolanaBlock(solanaAffectedSlot, []byte(solanaAffectedBlock), timestamp)
	if err != nil {
		t.Fatalf("parse block: %v", err)
	}
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("expected one transfer, got %#v", batches)
	}
	got := batches[0][0]
	if got.Amount.String() != "1.6" {
		t.Fatalf("amount = %s, want 1.6", got.Amount)
	}
	if got.FromAddress != "EDCqa2hCeToyBWJMGzJV1d5Qcez2HMHoYdzSS4tnvBtz" {
		t.Fatalf("sender owner = %s", got.FromAddress)
	}
	if got.RecvAddress != "D6rH1HeA3YGsMfSoFAm2FAcWuB1KUq7kuLcLz2Ka2snY" {
		t.Fatalf("destination owner = %s", got.RecvAddress)
	}
	if got.TradeType != model.UsdtSolana || got.Network != conf.Solana {
		t.Fatalf("unexpected route: trade=%s network=%s", got.TradeType, got.Network)
	}
	if got.BlockNum != solanaAffectedSlot || !got.Timestamp.Equal(timestamp) {
		t.Fatalf("unexpected block metadata: slot=%d time=%s", got.BlockNum, got.Timestamp)
	}
	if got.TxHash != "5CM3S19FtpxfFQsQmZNwcadm46MMEqr2Zb9jH7UFEv9edbKGboFtrihKxBQuvxqWRm7p78dJpXxXpHR5PWTpDykL" {
		t.Fatalf("transaction hash = %s", got.TxHash)
	}
}

func TestFetchSolanaBlockRejectsUnusableRPCResponses(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "json rpc error", body: `{"jsonrpc":"2.0","error":{"code":-32004,"message":"block unavailable"},"id":1}`},
		{name: "null result", body: `{"jsonrpc":"2.0","result":null,"id":1}`},
		{name: "missing block time", body: `{"jsonrpc":"2.0","result":{"transactions":[]},"id":1}`},
		{name: "zero block time", body: `{"jsonrpc":"2.0","result":{"blockTime":0,"transactions":[]},"id":1}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSolana()
			s.client = solanaClientWithBody(http.StatusOK, tt.body)
			if _, _, err := s.fetchSolanaBlock(solanaAffectedSlot); err == nil {
				t.Fatal("expected unusable response to fail")
			}
		})
	}
}

func TestInvalidSolanaRPCSchedulesOneRetryWithoutSuccess(t *testing.T) {
	if err := log.Init(t.TempDir()); err != nil {
		t.Fatalf("init test logger: %v", err)
	}
	defer log.Close()
	tests := []struct {
		name string
		body string
	}{
		{name: "json rpc error", body: `{"jsonrpc":"2.0","error":{"code":-32004,"message":"block unavailable"},"id":1}`},
		{name: "null result", body: `{"jsonrpc":"2.0","result":null,"id":1}`},
		{name: "missing block time", body: `{"jsonrpc":"2.0","result":{"transactions":[]},"id":1}`},
		{name: "zero block time", body: `{"jsonrpc":"2.0","result":{"blockTime":0,"transactions":[]},"id":1}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSolana()
			s.client = solanaClientWithBody(http.StatusOK, tt.body)
			s.retryDelays = []time.Duration{time.Hour}
			var successes, failures int
			s.recordSuccess = func(string, string) { successes++ }
			s.recordFailure = func(string) { failures++ }

			s.slotParse(solanaAffectedSlot)
			s.slotParse(solanaAffectedSlot)

			s.retryMu.Lock()
			pending := s.retryPending[solanaAffectedSlot]
			attempts := s.retryAttempts[solanaAffectedSlot]
			s.retryMu.Unlock()
			if successes != 0 || failures != 2 {
				t.Fatalf("metric calls: successes=%d failures=%d", successes, failures)
			}
			if !pending || attempts != 1 {
				t.Fatalf("retry state: pending=%v attempts=%d", pending, attempts)
			}
			s.clearRetry(solanaAffectedSlot)
		})
	}
}

func TestSolanaRetryBurstIsBoundedAndSuccessClearsState(t *testing.T) {
	s := newSolana()
	s.retryDelays = []time.Duration{time.Hour, time.Hour}
	for i := 0; i < 2; i++ {
		if !s.scheduleRetry(solanaAffectedSlot) {
			t.Fatalf("retry %d should be scheduled", i+1)
		}
		s.retryMu.Lock()
		s.retryPending[solanaAffectedSlot] = false
		if timer := s.retryTimers[solanaAffectedSlot]; timer != nil {
			timer.Stop()
		}
		delete(s.retryTimers, solanaAffectedSlot)
		s.retryMu.Unlock()
	}
	if s.scheduleRetry(solanaAffectedSlot) {
		t.Fatal("retry burst must stop after configured delays")
	}
	s.clearRetry(solanaAffectedSlot)
	s.retryMu.Lock()
	_, attemptsRemain := s.retryAttempts[solanaAffectedSlot]
	_, pendingRemain := s.retryPending[solanaAffectedSlot]
	s.retryMu.Unlock()
	if attemptsRemain || pendingRemain {
		t.Fatal("success must clear retry state")
	}
}
