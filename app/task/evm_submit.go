package task

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/utils"
)

type SubmittedTransactionState string

const (
	SubmittedTransactionPending    SubmittedTransactionState = "pending"
	SubmittedTransactionConfirming SubmittedTransactionState = "confirming"
	SubmittedTransactionConfirmed  SubmittedTransactionState = "confirmed"
)

var (
	ErrSubmittedTransactionInvalid     = errors.New("submitted transaction does not match the order")
	ErrSubmittedTransactionUnsupported = errors.New("transaction hash submission is not supported for this payment type")
	evmHashPattern                     = regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)
	evmVerifiers                       sync.Map
)

type SubmittedTransactionResult struct {
	State                 SubmittedTransactionState
	OrderStatus           int
	Confirmations         int
	RequiredConfirmations int
	ReceiptKey            string
	Transfer              transfer
}

func registerEVMVerifier(e *evm) {
	evmVerifiers.Store(model.Network(e.Network), e)
}

func NormalizeEVMHash(hash string) (string, bool) {
	hash = strings.TrimSpace(hash)
	if !evmHashPattern.MatchString(hash) {
		return "", false
	}
	return strings.ToLower(hash), true
}

// SubmitEVMTransaction verifies one customer-provided transaction instead of
// scanning an entire chain. Provider failures stay pending and never create a
// settlement claim.
func SubmitEVMTransaction(ctx context.Context, order model.Order, rawHash string) (SubmittedTransactionResult, error) {
	config, ok := model.GetTradeConfig(order.TradeType)
	if !ok || !model.HashSubmissionEnabled(order.TradeType) {
		return SubmittedTransactionResult{}, ErrSubmittedTransactionUnsupported
	}
	verifierValue, ok := evmVerifiers.Load(config.Network)
	if !ok {
		return SubmittedTransactionResult{State: SubmittedTransactionPending, OrderStatus: order.Status}, nil
	}
	hash, ok := NormalizeEVMHash(rawHash)
	if !ok {
		return SubmittedTransactionResult{}, ErrSubmittedTransactionInvalid
	}

	if order.Status == model.OrderStatusConfirming || order.Status == model.OrderStatusSuccess {
		if strings.EqualFold(order.RefHash, hash) {
			state := SubmittedTransactionConfirming
			if order.Status == model.OrderStatusSuccess {
				state = SubmittedTransactionConfirmed
			}
			return SubmittedTransactionResult{State: state, OrderStatus: order.Status, ReceiptKey: order.RefReceiptKey}, nil
		}
		return SubmittedTransactionResult{}, model.ErrOrderStateConflict
	}

	result, err := verifierValue.(*evm).inspectSubmittedTransaction(ctx, order, hash)
	if err != nil || result.State == SubmittedTransactionPending {
		return result, err
	}
	if err := order.MarkConfirmingReceipt(
		result.Transfer.BlockNum,
		result.Transfer.FromAddress,
		hash,
		result.ReceiptKey,
		result.Transfer.Timestamp,
		result.Transfer.Amount,
	); err != nil {
		return SubmittedTransactionResult{}, err
	}
	result.OrderStatus = model.OrderStatusConfirming
	if result.State == SubmittedTransactionConfirmed {
		if markFinalConfirmed(order) {
			result.OrderStatus = model.OrderStatusSuccess
		} else {
			result.State = SubmittedTransactionConfirming
		}
	}
	return result, nil
}

func (e *evm) inspectSubmittedTransaction(ctx context.Context, order model.Order, hash string) (SubmittedTransactionResult, error) {
	result := SubmittedTransactionResult{
		State:                 SubmittedTransactionPending,
		OrderStatus:           order.Status,
		RequiredConfirmations: e.Block.ConfirmedOffset,
	}
	chainBody, err := e.rpcRequest(ctx, "eth_chainId", "", []byte(
		`{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}`), rpcResultAny)
	if err != nil {
		return result, nil
	}
	if !strings.EqualFold(gjson.ParseBytes(chainBody).Get("result").String(), expectedEVMChainID(e.Network)) {
		return result, ErrSubmittedTransactionInvalid
	}
	receiptBody, err := e.rpcRequest(ctx, "eth_getTransactionReceipt", "", []byte(fmt.Sprintf(
		`{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["%s"],"id":1}`, hash)), rpcResultAllowNull)
	if err != nil {
		return result, nil
	}
	receipt := gjson.ParseBytes(receiptBody).Get("result")
	if receipt.Type == gjson.Null {
		return result, nil
	}
	if receipt.Get("status").String() != "0x1" || !strings.EqualFold(receipt.Get("transactionHash").String(), hash) {
		return result, ErrSubmittedTransactionInvalid
	}
	blockHex := receipt.Get("blockNumber").String()
	blockNum := int(utils.HexStr2Int(blockHex).Int64())
	if blockNum <= 0 {
		return result, nil
	}

	config, ok := model.GetTradeConfig(order.TradeType)
	if !ok || config.Contract == "" {
		return result, ErrSubmittedTransactionUnsupported
	}
	matches := make([]transfer, 0, 1)
	receiptKeys := make([]string, 0, 1)
	for _, event := range receipt.Get("logs").Array() {
		if !strings.EqualFold(event.Get("address").String(), config.Contract) {
			continue
		}
		topics := event.Get("topics").Array()
		if len(topics) < 3 || !strings.EqualFold(topics[0].String(), evmTransferEvent) {
			continue
		}
		from, fromOK := evmTopicAddress(topics[1].String())
		recv, recvOK := evmTopicAddress(topics[2].String())
		amount, amountOK := evmEventAmount(event.Get("data").String(), config.Decimal)
		if !fromOK || !recvOK || !amountOK || !strings.EqualFold(recv, orderMatchAddress(order)) {
			continue
		}
		expected, amountErr := decimal.NewFromString(order.Amount)
		if amountErr != nil || !amount.Equal(expected) {
			continue
		}
		logIndex, logIndexOK := canonicalEVMLogIndex(event.Get("logIndex").String())
		if !logIndexOK {
			continue
		}
		matches = append(matches, transfer{
			Network:     e.Network,
			TxHash:      hash,
			Amount:      amount,
			FromAddress: from,
			RecvAddress: recv,
			TradeType:   order.TradeType,
			BlockNum:    blockNum,
		})
		receiptKeys = append(receiptKeys, evmReceiptKey(e.Network, hash, logIndex))
	}
	if len(matches) != 1 {
		return result, ErrSubmittedTransactionInvalid
	}

	blockBody, err := e.rpcRequest(ctx, "eth_getBlockByNumber", blockHex, []byte(fmt.Sprintf(
		`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["%s",false],"id":1}`, blockHex)), rpcResultObject)
	if err != nil {
		return result, nil
	}
	blockTimestamp := utils.HexStr2Int(gjson.ParseBytes(blockBody).Get("result.timestamp").String()).Int64()
	if blockTimestamp <= 0 {
		return result, nil
	}
	matches[0].Timestamp = time.Unix(blockTimestamp, 0)
	if !submittedTransferWithinOrder(order, matches[0].Timestamp) {
		return result, ErrSubmittedTransactionInvalid
	}

	headBody, err := e.rpcRequest(ctx, "eth_blockNumber", "", []byte(
		`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), rpcResultAny)
	if err != nil {
		return result, nil
	}
	headHex := gjson.ParseBytes(headBody).Get("result").String()
	if !strings.HasPrefix(headHex, "0x") {
		return result, nil
	}
	head := int(utils.HexStr2Int(headHex).Int64())
	if head < blockNum {
		return result, nil
	}
	result.Transfer = matches[0]
	result.ReceiptKey = receiptKeys[0]
	result.State = SubmittedTransactionConfirming
	result.Confirmations = head - blockNum
	if result.Confirmations >= result.RequiredConfirmations {
		result.State = SubmittedTransactionConfirmed
	}
	return result, nil
}

func expectedEVMChainID(network string) string {
	switch network {
	case conf.Bsc:
		return "0x38"
	case conf.Polygon:
		return "0x89"
	default:
		return ""
	}
}

func evmTopicAddress(topic string) (string, bool) {
	if len(topic) != 66 || !strings.HasPrefix(topic, "0x") {
		return "", false
	}
	return "0x" + strings.ToLower(topic[26:]), true
}

func evmEventAmount(data string, decimals int32) (decimal.Decimal, bool) {
	if len(data) < 3 || !strings.HasPrefix(data, "0x") {
		return decimal.Zero, false
	}
	value, ok := new(big.Int).SetString(data[2:], 16)
	if !ok || value.Sign() <= 0 {
		return decimal.Zero, false
	}
	return decimal.NewFromBigInt(value, decimals), true
}

func evmReceiptKey(network, hash, logIndex string) string {
	return strings.ToLower(strings.TrimSpace(network) + ":" + strings.TrimSpace(hash) + ":" + strings.TrimSpace(logIndex))
}

func canonicalEVMLogIndex(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 3 || !strings.HasPrefix(raw, "0x") {
		return "", false
	}
	value, ok := new(big.Int).SetString(raw[2:], 16)
	if !ok || value.Sign() < 0 {
		return "", false
	}
	return "0x" + value.Text(16), true
}

func submittedTransferWithinOrder(order model.Order, at time.Time) bool {
	if order.CreatedAt == nil || at.Before(order.CreatedAt.Time()) || !at.Before(order.ExpiredAt) {
		return false
	}
	if order.Status == model.OrderStatusCanceled && order.UpdatedAt != nil && !at.Before(order.UpdatedAt.Time()) {
		return false
	}
	return true
}
