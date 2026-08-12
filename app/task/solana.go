package task

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil/base58"
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

// 参考文档
//  - https://solana.com/zh/docs/rpc
//  - https://github.com/solana-program/token/blob/6d18ff73b1dd30703a30b1ca941cb0f1d18c2b2a/program/src/instruction.rs

type solana struct {
	slotConfirmedOffset int
	lastSlotNum         int
	slotQueue           *chanx.UnboundedChan[int]
	client              *http.Client
	retryMu             sync.Mutex
	retryAttempts       map[int]int
	retryPending        map[int]bool
	retryTimers         map[int]*time.Timer
	retryDelays         []time.Duration
	recordSuccess       func(string, string)
	recordFailure       func(string)
}

type solanaTokenOwner struct {
	TradeType model.TradeType
	Address   string
}

var sol solana

func init() {
	sol = newSolana()
	Register(Task{Callback: sol.slotDispatch})
	Register(Task{Callback: sol.syncSlotForward, Duration: time.Second * 5})
	Register(Task{Callback: sol.tradeConfirmHandle, Duration: time.Second * 5})
	Register(Task{Callback: sol.lookbackSlots, Duration: time.Second * 15})
}

func newSolana() solana {
	return solana{
		slotConfirmedOffset: 60,
		lastSlotNum:         0,
		slotQueue:           chanx.NewUnboundedChan[int](context.Background(), 30),
		client:              utils.NewHttpClient(),
		retryAttempts:       make(map[int]int),
		retryPending:        make(map[int]bool),
		retryTimers:         make(map[int]*time.Timer),
		retryDelays:         []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second},
		recordSuccess:       conf.RecordSuccess,
		recordFailure:       conf.RecordFailure,
	}
}

func (s *solana) syncSlotForward(ctx context.Context) {
	if syncBreak(conf.Solana, s.slotQueue.Len()) {

		return
	}

	req, _ := http.NewRequestWithContext(ctx, "POST", model.Endpoint(conf.Solana), bytes.NewBuffer([]byte(`{"jsonrpc":"2.0","id":1,"method":"getSlot"}`)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		log.Task.Warn("syncSlotForward Error sending request:", err)

		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Task.Warn("syncSlotForward Error response status code:", resp.StatusCode)

		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Task.Warn("syncSlotForward Error reading response body:", err)

		return
	}

	now := int(gjson.GetBytes(body, "result").Int())
	if now <= 0 {
		log.Task.Warn("syncSlotForward Error: invalid slot number:", now)

		return
	}

	if now-s.lastSlotNum > cast.ToInt(model.GetC(model.BlockHeightMaxDiff)) { // 区块高度变化过大，强制丢块重扫
		s.lastSlotNum = now
	}

	if now == s.lastSlotNum { // 区块高度没有变化

		return
	}

	for n := s.lastSlotNum + 1; n <= now; n++ {
		// 待扫描区块入列

		s.slotQueue.In <- n
	}

	s.lastSlotNum = now
}

func (s *solana) slotDispatch(ctx context.Context) {
	p, err := ants.NewPoolWithFunc(3, s.slotParse)
	if err != nil {
		log.Task.Warn("Error creating pool:", err)

		return
	}

	defer p.Release()

	for {
		select {
		case slot := <-s.slotQueue.Out:
			if err := p.Invoke(slot); err != nil {
				s.slotQueue.In <- slot
				log.Task.Warn("slotDispatch Error invoking process slot:", err)
			}
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				log.Task.Warn("slotDispatch context done:", err)
			}

			return
		}
	}
}

func (s *solana) slotParse(n any) {
	slot := n.(int)
	network := conf.Solana

	body, timestamp, err := s.fetchSolanaBlock(slot)
	if err != nil {
		s.recordFailure(network)
		retryScheduled := s.scheduleRetry(slot)
		log.Task.Warn("slotParse Error fetching block:", err)
		if !retryScheduled {
			log.Task.Warn("slotParse retry burst exhausted for slot:", slot)
		}

		return
	}

	transfers, err := s.parseSolanaBlock(slot, body, timestamp)
	if err != nil {
		s.recordFailure(network)
		retryScheduled := s.scheduleRetry(slot)
		log.Task.Warn("slotParse Error parsing block:", err)
		if !retryScheduled {
			log.Task.Warn("slotParse retry burst exhausted for slot:", slot)
		}

		return
	}

	s.recordSuccess(network, cast.ToString(slot))
	s.clearRetry(slot)
	for _, result := range transfers {
		if len(result) > 0 {
			transferQueue.In <- result
		}
	}

	log.Task.Info(fmt.Sprintf("区块扫描完成(Solana) %d 成功率：%s", slot, conf.GetSuccessRate(network)))
}

type solanaBlockResult struct {
	BlockTime    *int64          `json:"blockTime"`
	Transactions json.RawMessage `json:"transactions"`
}

type solanaRPCResponse struct {
	Error  json.RawMessage  `json:"error"`
	Result *json.RawMessage `json:"result"`
}

func (s *solana) fetchSolanaBlock(slot int) ([]byte, time.Time, error) {
	post := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[%d,{"encoding":"json","maxSupportedTransactionVersion":0,"transactionDetails":"full","rewards":false}]}`, slot))
	resp, err := s.client.Post(model.Endpoint(conf.Solana), "application/json", bytes.NewBuffer(post))
	if err != nil {
		return nil, time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, time.Time{}, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, time.Time{}, err
	}
	var envelope solanaRPCResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, time.Time{}, fmt.Errorf("invalid JSON-RPC response: %w", err)
	}
	if len(envelope.Error) > 0 && string(envelope.Error) != "null" {
		return nil, time.Time{}, fmt.Errorf("JSON-RPC error: %s", string(envelope.Error))
	}
	if envelope.Result == nil || string(*envelope.Result) == "null" {
		return nil, time.Time{}, fmt.Errorf("JSON-RPC result is null")
	}
	var block solanaBlockResult
	if err := json.Unmarshal(*envelope.Result, &block); err != nil {
		return nil, time.Time{}, fmt.Errorf("invalid block result: %w", err)
	}
	if block.BlockTime == nil || *block.BlockTime <= 0 {
		return nil, time.Time{}, fmt.Errorf("missing or invalid blockTime")
	}
	if len(block.Transactions) == 0 || !gjson.ParseBytes(block.Transactions).IsArray() {
		return nil, time.Time{}, fmt.Errorf("missing or invalid transactions")
	}
	return body, time.Unix(*block.BlockTime, 0), nil
}

func (s *solana) parseSolanaBlock(slot int, body []byte, timestamp time.Time) ([][]transfer, error) {
	result := gjson.GetBytes(body, "result")
	if !result.Exists() || !result.IsObject() {
		return nil, fmt.Errorf("missing block result")
	}
	transfers := make([][]transfer, 0)

	for _, trans := range result.Get("transactions").Array() {
		hash := trans.Get("transaction.signatures.0").String()

		// 解析账号索引
		accountKeys := make([]string, 0)
		for _, key := range trans.Get("transaction.message.accountKeys").Array() {
			accountKeys = append(accountKeys, key.String())
		}
		for _, v := range []string{"readonly", "writable"} {
			for _, key := range trans.Get("meta.loadedAddresses." + v).Array() {
				accountKeys = append(accountKeys, key.String())
			}
		}

		// 查找SPL Token索引
		splTokenIndex := int64(-1)
		for i, v := range accountKeys {
			if v == conf.SolSplToken {
				splTokenIndex = int64(i)

				break
			}
		}

		// SPL Token的Mint地址，即不包含 Token 交易信息
		if splTokenIndex == -1 {

			continue
		}

		// 解析 Token 账户 【Token Wallet => Owner Wallet】
		tokenAccountMap := make(map[string]solanaTokenOwner)
		for _, v := range []string{"postTokenBalances", "preTokenBalances"} {
			for _, itm := range trans.Get("meta." + v).Array() {
				tradeType, ok := model.GetContractTrade(itm.Get("mint").String())
				if !ok || itm.Get("programId").String() != conf.SolSplToken {

					continue
				}

				tokenAccountMap[accountKeys[itm.Get("accountIndex").Int()]] = solanaTokenOwner{
					TradeType: tradeType,
					Address:   itm.Get("owner").String(),
				}
			}
		}

		transArr := make([]transfer, 0)

		// 解析外部指令
		for _, instr := range trans.Get("transaction.message.instructions").Array() {
			if instr.Get("programIdIndex").Int() != splTokenIndex {

				continue
			}

			transArr = append(transArr, s.parseTransfer(instr, accountKeys, tokenAccountMap))
		}

		// 解析内部指令
		for _, itm := range trans.Get("meta.innerInstructions").Array() {
			for _, instr := range itm.Get("instructions").Array() {
				if instr.Get("programIdIndex").Int() != splTokenIndex {

					continue
				}

				transArr = append(transArr, s.parseTransfer(instr, accountKeys, tokenAccountMap))
			}
		}

		// 过滤无关交易
		result := make([]transfer, 0)
		for _, t := range transArr {
			if t.FromAddress == "" || t.RecvAddress == "" || t.Amount.IsZero() {

				continue
			}

			t.TxHash = hash
			t.Network = conf.Solana
			t.BlockNum = slot
			t.Timestamp = timestamp

			result = append(result, t)
		}

		if len(result) > 0 {
			transfers = append(transfers, result)
		}
	}
	return transfers, nil
}

func (s *solana) clearRetry(slot int) {
	s.retryMu.Lock()
	if timer := s.retryTimers[slot]; timer != nil {
		timer.Stop()
	}
	delete(s.retryAttempts, slot)
	delete(s.retryPending, slot)
	delete(s.retryTimers, slot)
	s.retryMu.Unlock()
}

func (s *solana) scheduleRetry(slot int) bool {
	s.retryMu.Lock()
	if s.retryPending[slot] {
		s.retryMu.Unlock()
		return true
	}
	attempt := s.retryAttempts[slot]
	if attempt >= len(s.retryDelays) {
		delete(s.retryAttempts, slot)
		delete(s.retryPending, slot)
		delete(s.retryTimers, slot)
		s.retryMu.Unlock()
		return false
	}
	delay := s.retryDelays[attempt]
	s.retryAttempts[slot] = attempt + 1
	s.retryPending[slot] = true
	var timer *time.Timer
	timer = time.AfterFunc(delay, func() {
		s.retryMu.Lock()
		if s.retryTimers[slot] != timer {
			s.retryMu.Unlock()
			return
		}
		delete(s.retryPending, slot)
		delete(s.retryTimers, slot)
		s.retryMu.Unlock()
		s.slotQueue.In <- slot
	})
	s.retryTimers[slot] = timer
	s.retryMu.Unlock()
	return true
}

func (s *solana) parseTransfer(instr gjson.Result, accountKeys []string, tokenAccountMap map[string]solanaTokenOwner) transfer {
	accounts := instr.Get("accounts").Array()
	trans := transfer{}
	if len(accounts) < 3 { // from to singer，至少存在3个账户索引，如果是多签则 > 3

		return trans
	}

	data := base58.Decode(instr.Get("data").String())
	dLen := len(data)
	if dLen < 9 {

		return trans
	}

	isTransfer := data[0] == 3 && dLen == 9
	isTransferChecked := data[0] == 12 && dLen == 10
	if !isTransfer && !isTransferChecked {

		return trans
	}

	var exp int32 = -6
	if isTransferChecked {
		exp = int32(data[9]) * -1
	}

	from, ok := tokenAccountMap[accountKeys[accounts[0].Int()]]
	if !ok {

		return trans
	}

	trans.FromAddress = from.Address
	trans.RecvAddress = tokenAccountMap[accountKeys[accounts[1].Int()]].Address
	if isTransferChecked {
		trans.RecvAddress = tokenAccountMap[accountKeys[accounts[2].Int()]].Address
	}

	buf := make([]byte, 8)
	copy(buf[:], data[1:9])
	number := binary.LittleEndian.Uint64(buf)
	b := new(big.Int)
	b.SetUint64(number)
	trans.TradeType = from.TradeType
	trans.Amount = decimal.NewFromBigInt(b, exp)

	return trans
}

func (s *solana) tradeConfirmHandle(ctx context.Context) {
	var orders = getConfirmingOrders(model.GetNetworkTrades(conf.Solana))
	var wg sync.WaitGroup

	var handle = func(o model.Order) {
		if model.GetC(model.BlockOffsetConfirm) == "1" {
			if s.lastSlotNum == 0 {
				return
			}
			if s.lastSlotNum-o.RefBlockNum < s.slotConfirmedOffset {
				return
			}
		}

		post := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getSignatureStatuses","params":[["%s"],{"searchTransactionHistory":true}]}`, o.RefHash))
		req, _ := http.NewRequestWithContext(ctx, "POST", model.Endpoint(conf.Solana), bytes.NewBuffer(post))
		resp, err := s.client.Do(req)
		if err != nil {
			log.Task.Warn("solana tradeConfirmHandle Error sending request:", err)

			return
		}

		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			log.Task.Warn("solana tradeConfirmHandle Error response status code:", resp.StatusCode)

			return
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Task.Warn("solana tradeConfirmHandle Error reading response body:", err)

			return
		}

		data := gjson.ParseBytes(body)
		if data.Get("error").Exists() {
			log.Task.Warn("solana tradeConfirmHandle Error:", data.Get("error").String())

			return
		}

		if data.Get("result.value.0.confirmationStatus").String() == "finalized" {

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

func (s *solana) lookbackSlots(ctx context.Context) {
	if syncBreak(conf.Solana, s.slotQueue.Len()) {
		return
	}

	startAt, endAt, ok := getLookbackUnix(conf.Solana)
	if !ok {
		return
	}

	start, end := blockapi.New().GetBoundaryHeights(startAt, endAt, conf.Solana)
	for i := int(start); i <= int(end); i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if syncBreak(conf.Solana, s.slotQueue.Len()) {
			return
		}
		s.slotQueue.In <- i
		time.Sleep(time.Millisecond * 200)
	}
}
