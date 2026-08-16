package task

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"github.com/smallnest/chanx"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/notifier"
	"github.com/v03413/bepusdt/app/task/notify"
	"github.com/v03413/tronprotocol/core"
)

type transfer struct {
	Network     string          `json:"network"`
	TxHash      string          `json:"tx_hash"`
	Amount      decimal.Decimal `json:"amount"`
	FromAddress string          `json:"from_address"`
	RecvAddress string          `json:"recv_address"`
	Timestamp   time.Time       `json:"timestamp"`
	TradeType   model.TradeType `json:"trade_type"`
	BlockNum    int             `json:"block_num"`
	ReceiptKey  string          `json:"receipt_key"`
}

type resource struct {
	ID           string
	Type         core.Transaction_Contract_ContractType
	Balance      int64
	FromAddress  string
	RecvAddress  string
	Timestamp    time.Time
	ResourceCode core.ResourceCode
}

var resourceQueue = chanx.NewUnboundedChan[[]resource](context.Background(), 30) // 资源队列
var notOrderQueue = chanx.NewUnboundedChan[[]transfer](context.Background(), 30) // 非订单队列
var transferQueue = chanx.NewUnboundedChan[[]transfer](context.Background(), 30) // 交易转账队列

// lookbackAttempt throttles repeated scans; queued work is not treated as complete.
var lookbackAttempt sync.Map // key: int64 order ID, value: time.Time

const (
	normalLookbackThrottle         = 2 * time.Minute
	reconciliationLookbackThrottle = 6 * time.Hour
)

const batchInterval = time.Second * 1       // 批处理缓解数据库读取压力
const orderCheckInterval = time.Second * 10 // 订单过期检查间隔

func init() {
	Register(Task{Callback: orderTransferHandle})
	Register(Task{Callback: notOrderTransferHandle})
	Register(Task{Callback: tronResourceHandle})
}

func orderTransferHandle(ctx context.Context) {
	var batch = make([]transfer, 0, 1000)
	var lastCheckTime = time.Now()
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case transfers, ok := <-transferQueue.Out:
			if !ok {
				return
			}
			batch = append(batch, transfers...)
		case <-ticker.C:
			// 每10秒强制检查一次过期订单，即使没有交易，防止无交易时订单不过期
			var shouldCheck = time.Since(lastCheckTime) >= orderCheckInterval
			if shouldCheck {
				lastCheckTime = time.Now()
			}

			if len(batch) == 0 {
				if shouldCheck {
					expireWaitingOrders()
				}

				continue
			}

			var other = make([]transfer, 0)
			var orders = getReceivableOrders()

			for _, t := range batch {
				// 判断数额是否在允许范围内
				if !model.IsAmountValid(t.TradeType, t.Amount) {
					continue
				}

				mqttPublish(t)

				key := fmt.Sprintf("%s%s", t.RecvAddress, t.TradeType)
				orderList, ok := orders[key]
				if !ok {
					other = append(other, t)
					continue
				}

				var matched bool
				for i, o := range orderList {
					if !orderTransferMatch(o, t) {
						continue
					}

					// 订单匹配 进入确认流程
					if err := o.MarkConfirmingReceipt(t.BlockNum, t.FromAddress, t.TxHash, t.ReceiptKey, t.Timestamp, t.Amount); err != nil {
						log.Task.Warn("mark order confirming failed:", err)
						continue
					}

					// 从内存 map 中移除已匹配订单，防止同批次其他 transfer 重复匹配
					orders[key] = append(orderList[:i], orderList[i+1:]...)
					matched = true
					break
				}

				if !matched {
					other = append(other, t)
				}
			}

			if len(other) > 0 {
				notOrderQueue.In <- other
			}

			batch = batch[:0]

			if shouldCheck {
				expireWaitingOrders()
			}
		}
	}
}

func orderTransferMatch(o model.Order, t transfer) bool {
	if o.TradeType != t.TradeType || orderMatchAddress(o) != t.RecvAddress {
		return false
	}
	if !o.AddressLocked && !amountMatch(t.Amount, o.Amount, string(o.TradeType)) {
		return false
	}
	if !o.CreatedAt.Before(t.Timestamp) || !o.ExpiredAt.After(t.Timestamp) {
		return false
	}
	if o.Status == model.OrderStatusCanceled && o.UpdatedAt != nil && !t.Timestamp.Before(o.UpdatedAt.Time()) {
		return false
	}

	return true
}

func orderMatchAddress(o model.Order) string {
	if o.MatchAddress != "" {
		return o.MatchAddress
	}

	return o.Address
}

func notOrderTransferHandle(ctx context.Context) {
	var batch = make([]transfer, 0, 1000)
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case transfers, ok := <-notOrderQueue.Out:
			if !ok {
				return
			}
			batch = append(batch, transfers...)
		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}

			var was = make([]model.Wallet, 0)
			model.Db.Where("other_notify = ?", model.WaOtherEnable).Find(&was)
			for _, wa := range was {
				for _, t := range batch {
					if t.RecvAddress != wa.MatchAddr && t.FromAddress != wa.MatchAddr {
						continue
					}

					if !model.IsNeedNotifyByTxid(t.TxHash) {
						continue
					}

					var record = model.NotifyRecord{Txid: t.TxHash}
					model.Db.Create(&record)

					notifier.NonOrderTransfer(model.TronTransfer{
						Network:     t.Network,
						TxHash:      t.TxHash,
						Amount:      t.Amount,
						FromAddress: t.FromAddress,
						RecvAddress: t.RecvAddress,
						Timestamp:   t.Timestamp,
						TradeType:   t.TradeType,
						BlockNum:    t.BlockNum,
					}, wa)
				}
			}

			batch = batch[:0]
		}
	}
}

func tronResourceHandle(ctx context.Context) {
	var batch = make([]resource, 0, 1000)
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case resources, ok := <-resourceQueue.Out:
			if !ok {
				return
			}
			batch = append(batch, resources...)
		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}

			var was []model.Wallet
			model.Db.Where("status = ? and other_notify = ?", model.WaStatusEnable, model.WaOtherEnable).Find(&was)

			for _, wa := range was {
				if wa.GetNetwork() != conf.Tron {
					// 只有 Tron 网络目前才有资源变更通知
					continue
				}

				for _, r := range batch {
					if r.RecvAddress != wa.Address && r.FromAddress != wa.Address {
						continue
					}
					if r.ResourceCode != core.ResourceCode_ENERGY {
						continue
					}
					if !model.IsNeedNotifyByTxid(r.ID) {
						continue
					}

					var record = model.NotifyRecord{Txid: r.ID}
					model.Db.Create(&record)

					notifier.TronResourceChange(model.TronResource(r))
				}
			}

			batch = batch[:0]
		}
	}
}

func markFinalConfirmed(o model.Order) bool {
	if strings.TrimSpace(o.RefHash) == "" || o.RefBlockNum <= 0 || o.ConfirmedAt == nil || o.ConfirmedAt.IsZero() {
		log.Task.Warn(fmt.Sprintf("settlement rejected provider_order_id=%d reason=missing_chain_metadata", o.ID))
		return false
	}

	result := model.Db.Model(&model.Order{}).
		Where("id = ? and status = ? and ref_hash = ?", o.ID, model.OrderStatusConfirming, o.RefHash).
		Update("status", model.OrderStatusSuccess)
	if result.Error != nil {
		log.Task.Warn("mark order successful failed:", result.Error)
		return false
	}
	if result.RowsAffected != 1 {
		return false
	}

	o.Status = model.OrderStatusSuccess
	log.Task.Info(fmt.Sprintf("order transition provider_order_id=%d from=%d to=%d block=%d has_tx_hash=true", o.ID, model.OrderStatusConfirming, model.OrderStatusSuccess, o.RefBlockNum))
	notifyOrderSuccess(o)
	return true
}

func receivableOrderStatuses() []int {
	return []int{model.OrderStatusWaiting, model.OrderStatusExpired, model.OrderStatusCanceled}
}

func getReceivableOrders() map[string][]model.Order {
	var orders []model.Order
	now := time.Now()
	normalCutoff, reconciliationCutoff := receivableCutoffs(now)
	db := model.Db.Where("status in (?)", receivableOrderStatuses()).
		Where("expired_at > ?", earlierTime(normalCutoff, reconciliationCutoff)).
		Order("created_at asc")
	db.Find(&orders)

	data := make(map[string][]model.Order)
	excluded := reconciliationExcludedOrderIDs(model.GetC(model.PaymentReconciliationExcludeOrderIDs))
	for _, t := range orders {
		if !receivableOrderEligible(t, normalCutoff, reconciliationCutoff, excluded) {
			continue
		}
		key := orderMatchAddress(t) + string(t.TradeType)
		data[key] = append(data[key], t)
	}

	return data
}

func hasLookbackOrders(tradeType []model.TradeType) bool {
	now := time.Now()
	normalCutoff, reconciliationCutoff := receivableCutoffs(now)
	var orders []model.Order
	db := model.Db.Model(&model.Order{}).
		Where("status in (?)", receivableOrderStatuses()).
		Where("expired_at > ?", earlierTime(normalCutoff, reconciliationCutoff))
	if len(tradeType) > 0 {
		db = db.Where("trade_type in (?)", tradeType)
	}
	db.Find(&orders)

	excluded := reconciliationExcludedOrderIDs(model.GetC(model.PaymentReconciliationExcludeOrderIDs))
	for _, order := range orders {
		if receivableOrderEligible(order, normalCutoff, reconciliationCutoff, excluded) {
			return true
		}
	}

	return false
}

func getLookbackUnix(network model.Network) (startAt, endAt int64, ok bool) {
	trade := model.GetNetworkTrades(network)
	if len(trade) == 0 {
		return
	}

	now := time.Now()
	normalCutoff, reconciliationCutoff := receivableCutoffs(now)
	var all []model.Order
	model.Db.Model(&model.Order{}).
		Where("status in (?) and trade_type in (?)", receivableOrderStatuses(), trade).
		Where("expired_at > ?", earlierTime(normalCutoff, reconciliationCutoff)).
		Order("created_at asc").
		Find(&all)

	excluded := reconciliationExcludedOrderIDs(model.GetC(model.PaymentReconciliationExcludeOrderIDs))
	pending := make([]model.Order, 0, len(all))
	for _, o := range all {
		if !receivableOrderEligible(o, normalCutoff, reconciliationCutoff, excluded) {
			continue
		}
		throttle := normalLookbackThrottle
		if !o.ExpiredAt.After(normalCutoff) {
			throttle = reconciliationLookbackThrottle
		}
		if value, found := lookbackAttempt.Load(o.ID); found {
			if attemptedAt, valid := value.(time.Time); valid && now.Sub(attemptedAt) < throttle {
				continue
			}
		}
		pending = append(pending, o)
	}
	if len(pending) == 0 {
		return
	}

	// 起点：最早的创建时间（已按 created_at asc 排序）
	startAt = pending[0].CreatedAt.Time().Unix()

	// 终点：最晚的已过期 expired_at；若全部尚未过期则用当前时间
	endAt = startAt
	needsCurrentTip := false
	for _, o := range pending {
		if !o.ExpiredAt.Before(now) {
			needsCurrentTip = true
		}
		if o.ExpiredAt.Unix() > endAt {
			endAt = o.ExpiredAt.Unix()
		}
	}
	if needsCurrentTip {
		endAt = now.Unix()
	}

	ok = true

	// This is only an attempt timestamp, not reconciliation completion.
	for _, o := range pending {
		lookbackAttempt.Store(o.ID, now)
	}

	return
}

func receivableCutoffs(now time.Time) (time.Time, time.Time) {
	normalCutoff := now.Add(model.GetLookbackHour())
	reconciliationLookback := model.GetReconciliationLookbackHour()
	if reconciliationLookback == 0 {
		return normalCutoff, time.Time{}
	}
	return normalCutoff, now.Add(reconciliationLookback)
}

func earlierTime(a, b time.Time) time.Time {
	if b.IsZero() {
		return a
	}
	if a.Before(b) {
		return a
	}
	return b
}

func reconciliationExcludedOrderIDs(raw string) map[string]struct{} {
	excluded := make(map[string]struct{})
	for _, orderID := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\r'
	}) {
		orderID = strings.TrimSpace(orderID)
		if orderID != "" {
			excluded[orderID] = struct{}{}
		}
	}
	return excluded
}

func receivableOrderEligible(order model.Order, normalCutoff, reconciliationCutoff time.Time, excluded map[string]struct{}) bool {
	if _, found := excluded[strings.TrimSpace(order.OrderId)]; found {
		return false
	}
	if order.ExpiredAt.After(normalCutoff) {
		return true
	}
	return !reconciliationCutoff.IsZero() &&
		order.Status == model.OrderStatusExpired &&
		order.TradeType == model.UsdtBep20 &&
		order.ExpiredAt.After(reconciliationCutoff)
}

func expireWaitingOrders() {
	for _, t := range model.GetOrderByStatus(model.OrderStatusWaiting) {
		if time.Now().Unix() < t.ExpiredAt.Unix() {
			continue
		}

		t.SetExpired()
		notify.Bepusdt(t)
	}
}

func getConfirmingOrders(tradeType []model.TradeType) []model.Order {
	var orders = make([]model.Order, 0)
	var data = make([]model.Order, 0)
	var db = model.Db.Where("status = ?", model.OrderStatusConfirming)
	if len(tradeType) > 0 {
		db = db.Where("trade_type in (?)", tradeType)
	}

	db.Find(&orders)

	for _, order := range orders {
		if time.Now().Unix() >= order.ExpiredAt.Unix() {
			if order.ConfirmedAt == nil || order.ConfirmedAt.IsZero() || !order.ConfirmedAt.Before(order.ExpiredAt) {
				order.SetFailed()
				notify.Bepusdt(order)

				continue
			}
		}

		data = append(data, order)
	}

	return data
}

func amountMatch(amount decimal.Decimal, target, tradeType string) bool {
	return amountMatchMode(amount, target, tradeType, model.MatchMode(model.GetC(model.PaymentMatchMode)))
}

func amountMatchMode(amount decimal.Decimal, target, tradeType string, mode model.MatchMode) bool {
	switch mode {
	case model.Classic:
		targetAmount, err := decimal.NewFromString(target)
		return err == nil && amount.Equal(targetAmount)
	case model.HasPrefix:
		s := amount.String()
		if !strings.HasPrefix(s, target) {
			return false
		}
		rest := s[len(target):]
		if rest == "" {
			return true
		}

		return strings.Contains(target, ".") || strings.HasPrefix(rest, ".")
	case model.RoundOff:
		t, err := decimal.NewFromString(target)
		if err != nil {
			log.Warn(err.Error())

			return false
		}

		_, precision := model.GetAtomicity(model.TradeType(tradeType)) // 标准精度
		precision2 := abs(t.Exponent())                                // 实际精度
		if precision2 != precision {
			precision = precision2
		}

		a := amount.Round(precision)
		t = t.Round(precision)

		return a.Equal(t)
	}

	return false
}

func abs(n int32) int32 {
	if n < 0 {
		return -n
	}
	return n
}
