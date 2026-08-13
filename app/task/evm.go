package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/shopspring/decimal"
	"github.com/smallnest/chanx"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/v03413/bepusdt/app/conf"
	blockapi "github.com/v03413/bepusdt/app/core"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/utils"
)

const (
	blockParseMaxNum       = 10 // 每次解析区块的最大数量
	evmTransferEvent       = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	rpcAttemptsPerEndpoint = 2
)

var chainBlockNum sync.Map

type block struct {
	RollDelayOffset int64 // 延迟偏移量，某些RPC节点如果不延迟，会报错 block is out of range，目前发现 https://rpc.xlayer.tech/ 存在此问题
	ConfirmedOffset int   // 确认偏移量，开启交易确认后，区块高度需要减去此值认为交易已确认
}

type evmNative struct {
	Parse     bool
	Decimal   int32
	TradeType model.TradeType
}

type evm struct {
	Network          string
	Block            block
	Native           evmNative
	Client           *http.Client
	blockScanQueue   *chanx.UnboundedChan[evmBlock]
	LookbackInterval time.Duration // 回溯时每批入队的间隔，控制 RPC 调用速率；默认 500ms
	RPCEndpoints     []string
	rpcMu            sync.Mutex
	rpcIndex         int
}

type rpcResultKind uint8

const (
	rpcResultAny rpcResultKind = iota
	rpcResultArray
	rpcResultObject
	rpcResultAllowNull
	rpcResultBatchObjects
)

type evmBlock struct {
	From int64
	To   int64
}

func (e *evm) syncBlocksForward(ctx context.Context) {
	if syncBreak(e.Network, e.blockScanQueue.Len()) {

		return
	}

	post := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	body, err := e.rpcRequest(ctx, "eth_blockNumber", "", post, rpcResultAny)
	if err != nil {
		return
	}

	result := gjson.ParseBytes(body).Get("result")
	blockHex := result.String()
	if result.Type != gjson.String || !strings.HasPrefix(blockHex, "0x") {
		log.Task.Warn(fmt.Sprintf("%s eth_blockNumber invalid result", e.Network))
		return
	}

	var now = utils.HexStr2Int(blockHex).Int64() - e.Block.RollDelayOffset
	if now <= 0 {

		return
	}

	var lastBlockNumber int64
	if v, ok := chainBlockNum.Load(e.Network); ok {
		lastBlockNumber = v.(int64)
	}

	if now-lastBlockNumber > cast.ToInt64(model.GetC(model.BlockHeightMaxDiff)) {

		lastBlockNumber = now - 1
	}

	chainBlockNum.Store(e.Network, now)
	if now <= lastBlockNumber {

		return
	}

	for _, blockRange := range chunkEVMBlockRange(lastBlockNumber+1, now, blockParseMaxNum) {
		e.blockScanQueue.In <- blockRange
	}
}

func (e *evm) lookbackBlocks(ctx context.Context) {
	if syncBreak(e.Network, e.blockScanQueue.Len()) {
		return
	}

	startAt, endAt, ok := getLookbackUnix(model.Network(e.Network))
	if !ok {
		return
	}

	interval := e.LookbackInterval
	if interval <= 0 {
		interval = time.Millisecond * 300
	}

	start, end := blockapi.New().GetBoundaryHeights(startAt, endAt, e.Network)
	for _, blockRange := range chunkEVMBlockRange(start, end, blockParseMaxNum) {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if syncBreak(e.Network, e.blockScanQueue.Len()) {
			return
		}
		e.blockScanQueue.In <- blockRange
		time.Sleep(interval)
	}
}

func chunkEVMBlockRange(from, to int64, size int64) []evmBlock {
	if from > to || size <= 0 {
		return nil
	}
	ranges := make([]evmBlock, 0, (to-from)/size+1)
	for start := from; start <= to; start += size {
		end := start + size - 1
		if end > to {
			end = to
		}
		ranges = append(ranges, evmBlock{From: start, To: end})
	}
	return ranges
}

func (e *evm) blockDispatch(ctx context.Context) {
	p, err := ants.NewPoolWithFunc(3, e.getBlockByNumber)
	if err != nil {
		log.Task.Warn("Error creating pool:", err)

		return
	}

	defer p.Release()

	for {
		select {
		case <-ctx.Done():
			return
		case n := <-e.blockScanQueue.Out:
			if err := p.Invoke(n); err != nil {
				e.blockScanQueue.In <- n

				log.Task.Warn("Evm Block Dispatch Error invoking process block:", err)
			}
		}
	}
}

func (e *evm) getBlockByNumber(a any) {
	b, ok := a.(evmBlock)
	if !ok {
		log.Task.Warn("Evm Block Parse Error: expected []int64, got", a)

		return
	}

	items := make([]string, 0)
	for i := b.From; i <= b.To; i++ {
		items = append(items, fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",%t],"id":%d}`, i, e.Native.Parse, i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	body, err := e.rpcRequest(ctx, "eth_getBlockByNumber", fmt.Sprintf("%d-%d", b.From, b.To), []byte(fmt.Sprintf(`[%s]`, strings.Join(items, ","))), rpcResultBatchObjects)
	if err != nil {
		conf.RecordFailure(e.Network)
		e.blockScanQueue.In <- b
		return
	}

	nativeTransfers := make([]transfer, 0)
	blockTimestamp := make(map[string]time.Time)
	blocks := gjson.ParseBytes(body).Array()
	if len(blocks) != int(b.To-b.From+1) {
		conf.RecordFailure(e.Network)
		e.blockScanQueue.In <- b
		log.Task.Warn(fmt.Sprintf("%s eth_getBlockByNumber incomplete range=%d-%d", e.Network, b.From, b.To))
		return
	}
	for _, itm := range blocks {
		if itm.Get("error").Exists() {
			conf.RecordFailure(e.Network)
			e.blockScanQueue.In <- b
			log.Task.Warn(fmt.Sprintf("%s eth_getBlockByNumber response error %s", e.Network, itm.Get("error").String()))

			return
		}
		if !itm.Get("result.number").Exists() || !itm.Get("result.timestamp").Exists() || !itm.Get("result.transactions").IsArray() {
			conf.RecordFailure(e.Network)
			e.blockScanQueue.In <- b
			log.Task.Warn(fmt.Sprintf("%s eth_getBlockByNumber invalid block shape range=%d-%d", e.Network, b.From, b.To))
			return
		}

		timestamp := utils.HexStr2Int(itm.Get("result.timestamp").String()).Int64()
		blockTime := time.Unix(timestamp, 0)
		blockNumHex := itm.Get("result.number").String()
		blockTimestamp[blockNumHex] = blockTime

		var array = itm.Get("result.transactions").Array()
		if e.Native.Parse && len(array) != 0 {

			nativeTransfers = append(nativeTransfers, e.parseNativeTransfer(array, int(utils.HexStr2Int(blockNumHex).Int64()), blockTime)...)
		}
	}

	transfers, err := e.parseEventTransfer(b, blockTimestamp)
	if err != nil {
		conf.RecordFailure(e.Network)
		e.blockScanQueue.In <- b
		log.Task.Warn("Evm Block Parse Error parsing block transfer:", err)

		return
	}
	conf.RecordSuccess(e.Network, cast.ToString(b.To))

	if len(nativeTransfers) > 0 {
		transferQueue.In <- nativeTransfers
	}
	if len(transfers) > 0 {
		transferQueue.In <- transfers
	}

	log.Task.Info(fmt.Sprintf("区块扫描完成(%s): %d → %d 成功率：%s", e.Network, b.From, b.To, conf.GetSuccessRate(e.Network)))
}

func (e *evm) parseNativeTransfer(array []gjson.Result, num int, timestamp time.Time) []transfer {
	nativeTransfers := make([]transfer, 0)
	for _, tx := range array {
		if tx.Get("input").String() != "0x" {
			// 非原生币交易

			continue
		}

		valStr := tx.Get("value").String()
		if valStr == "0x0" || len(valStr) < 3 {
			// 过滤 0 值交易

			continue
		}

		amount, ok := big.NewInt(0).SetString(valStr[2:], 16)
		if !ok || amount.Sign() <= 0 {

			continue
		}

		toAddress := tx.Get("to").String()
		if toAddress == "" { // 合约创建交易 to 为空

			continue
		}

		nativeTransfers = append(nativeTransfers, transfer{
			Network:     e.Network,
			FromAddress: tx.Get("from").String(),
			RecvAddress: toAddress,
			Amount:      decimal.NewFromBigInt(amount, e.Native.Decimal),
			TxHash:      tx.Get("hash").String(),
			BlockNum:    num,
			Timestamp:   timestamp,
			TradeType:   e.Native.TradeType,
		})
	}

	return nativeTransfers
}

func (e *evm) parseEventTransfer(b evmBlock, timestamp map[string]time.Time) ([]transfer, error) {
	transfers := make([]transfer, 0)
	post, err := e.eventTransferRequest(b)
	if err != nil {
		return transfers, errors.Join(errors.New("eth_getLogs Marshal Error"), err)
	}
	body, err := e.rpcRequest(context.Background(), "eth_getLogs", fmt.Sprintf("%d-%d", b.From, b.To), post, rpcResultArray)
	if err != nil {
		return transfers, errors.Join(errors.New("eth_getLogs request failed"), err)
	}

	for _, itm := range gjson.ParseBytes(body).Get("result").Array() {
		to := itm.Get("address").String()
		tradeType, ok := model.GetContractTrade(to)
		if !ok {

			continue
		}

		topics := itm.Get("topics").Array()
		if len(topics) < 3 {

			continue
		}

		if topics[0].String() != evmTransferEvent { // transfer event signature

			continue
		}
		if len(topics[1].String()) < 26 || len(topics[2].String()) < 26 || !strings.HasPrefix(itm.Get("data").String(), "0x") || itm.Get("transactionHash").String() == "" {
			continue
		}

		from := fmt.Sprintf("0x%s", topics[1].String()[26:])
		recv := fmt.Sprintf("0x%s", topics[2].String()[26:])
		amount, ok := big.NewInt(0).SetString(itm.Get("data").String()[2:], 16)
		if !ok || amount.Sign() <= 0 {

			continue
		}

		blockNumber := int(utils.HexStr2Int(itm.Get("blockNumber").String()).Int64())
		if blockNumber <= 0 {
			continue
		}
		transfers = append(transfers, transfer{
			Network:     e.Network,
			FromAddress: from,
			RecvAddress: recv,
			Amount:      decimal.NewFromBigInt(amount, model.GetContractDecimal(to)),
			TxHash:      itm.Get("transactionHash").String(),
			BlockNum:    blockNumber,
			Timestamp:   timestamp[itm.Get("blockNumber").String()],
			TradeType:   tradeType,
		})
	}

	return transfers, nil
}

func (e *evm) eventTransferRequest(b evmBlock) ([]byte, error) {
	filter := map[string]any{
		"fromBlock": fmt.Sprintf("0x%x", b.From),
		"toBlock":   fmt.Sprintf("0x%x", b.To),
		"topics":    []string{evmTransferEvent},
	}
	if contracts := model.GetNetworkContracts(model.Network(e.Network)); len(contracts) > 0 {
		filter["address"] = contracts
	}

	return json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getLogs",
		"params":  []any{filter},
		"id":      1,
	})
}

func (e *evm) tradeConfirmHandle(ctx context.Context) {
	var orders = getConfirmingOrders(model.GetNetworkTrades(model.Network(e.Network)))
	var wg sync.WaitGroup

	var handle = func(o model.Order) {
		if model.GetC(model.BlockOffsetConfirm) == "1" {
			last, ok := chainBlockNum.Load(e.Network)
			if !ok {
				return
			}
			if cast.ToInt(last)-o.RefBlockNum < e.Block.ConfirmedOffset {
				return
			}
		}

		post := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["%s"],"id":1}`, o.RefHash))
		body, err := e.rpcRequest(ctx, "eth_getTransactionReceipt", "", post, rpcResultAllowNull)
		if err != nil {
			log.Task.Warn("evm tradeConfirmHandle RPC request failed:", err)

			return
		}

		if gjson.ParseBytes(body).Get("result.status").String() == "0x1" {
			markFinalConfirmed(o)
		}
	}

	for _, order := range orders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handle(order)
		}()
	}

	wg.Wait()
}

func (e *evm) rpcEndpoint() string {
	endpoints := e.rpcEndpoints()
	if len(endpoints) == 0 {
		return ""
	}

	e.rpcMu.Lock()
	defer e.rpcMu.Unlock()

	return endpoints[e.rpcIndex%len(endpoints)]
}

func (e *evm) rpcEndpoints() []string {
	if len(e.RPCEndpoints) > 0 {
		return splitRPCEndpoints(e.RPCEndpoints...)
	}

	return splitRPCEndpoints(model.Endpoint(model.Network(e.Network)))
}

func splitRPCEndpoints(values ...string) []string {
	seen := make(map[string]struct{})
	endpoints := make([]string, 0, len(values))
	for _, value := range values {
		for _, endpoint := range strings.FieldsFunc(value, func(r rune) bool {
			return r == ',' || r == ';'
		}) {
			endpoint = strings.TrimSpace(endpoint)
			if endpoint == "" {
				continue
			}
			if _, ok := seen[endpoint]; ok {
				continue
			}
			seen[endpoint] = struct{}{}
			endpoints = append(endpoints, endpoint)
		}
	}

	return endpoints
}

func rpcEndpointLabel(raw string) string {
	parsed, err := url.Parse(raw)
	if err == nil && parsed.Host != "" {
		return parsed.Host
	}

	return "configured"
}

func (e *evm) rpcRequest(ctx context.Context, method, blockRange string, payload []byte, kind rpcResultKind) ([]byte, error) {
	endpoints := e.rpcEndpoints()
	if len(endpoints) == 0 {
		return nil, errors.New("no RPC endpoints configured")
	}

	e.rpcMu.Lock()
	start := e.rpcIndex % len(endpoints)
	e.rpcMu.Unlock()

	client := e.Client
	if client == nil {
		client = http.DefaultClient
	}

	var lastErr error
	for offset := 0; offset < len(endpoints); offset++ {
		idx := (start + offset) % len(endpoints)
		endpoint := endpoints[idx]
		for attempt := 1; attempt <= rpcAttemptsPerEndpoint; attempt++ {
			body, err := rpcRequestOnce(ctx, client, endpoint, payload, kind)
			if err == nil {
				e.rpcMu.Lock()
				e.rpcIndex = idx
				e.rpcMu.Unlock()

				return body, nil
			}

			lastErr = err
			log.Task.Warn(fmt.Sprintf("%s RPC failure endpoint=%s method=%s range=%s attempt=%d reason=%s", e.Network, rpcEndpointLabel(endpoint), method, blockRange, attempt, err.Error()))
			if attempt < rpcAttemptsPerEndpoint {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(200 * time.Millisecond):
				}
			}
		}
		if offset+1 < len(endpoints) {
			log.Task.Warn(fmt.Sprintf("%s RPC failover endpoint=%s method=%s range=%s", e.Network, rpcEndpointLabel(endpoint), method, blockRange))
		}
	}

	return nil, lastErr
}

func rpcRequestOnce(ctx context.Context, client *http.Client, endpoint string, payload []byte, kind rpcResultKind) ([]byte, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("HTTP status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if err := validateRPCResponse(body, kind); err != nil {
		return nil, err
	}

	return body, nil
}

func validateRPCResponse(body []byte, kind rpcResultKind) error {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return errors.New("malformed JSON")
	}
	root := gjson.ParseBytes(body)
	if kind == rpcResultBatchObjects {
		if !root.IsArray() || len(root.Array()) == 0 {
			return errors.New("missing batch result")
		}
		for _, item := range root.Array() {
			if !item.IsObject() || item.Get("error").Exists() || !item.Get("result").IsObject() {
				return errors.New("invalid batch item")
			}
		}
		return nil
	}
	if !root.IsObject() {
		return errors.New("invalid JSON-RPC envelope")
	}
	if root.Get("error").Exists() {
		return errors.New("JSON-RPC error")
	}
	result := root.Get("result")
	if !result.Exists() {
		return errors.New("missing result")
	}
	if kind == rpcResultArray && !result.IsArray() {
		return errors.New("invalid array result")
	}
	if kind == rpcResultObject && !result.IsObject() {
		return errors.New("invalid object result")
	}
	if kind != rpcResultAllowNull && result.Type == gjson.Null {
		return errors.New("null result")
	}

	return nil
}

func syncBreak(network string, num int) bool {
	if num >= blockQueueLimit {
		log.Task.Warn(fmt.Sprintf("%s 同步阻塞，当前区块消费堆积数量：%d", network, num))

		return true
	}

	if mqttSubscribed(network) {
		return false
	}

	trades := model.GetNetworkTrades(model.Network(network))
	if len(trades) == 0 {

		return true
	}

	var count int64
	model.Db.Model(&model.Wallet{}).
		Where("other_notify = ? and trade_type in (?)", model.WaOtherEnable, trades).
		Count(&count)
	if count > 0 {

		return false
	}

	return !hasLookbackOrders(trades)
}
