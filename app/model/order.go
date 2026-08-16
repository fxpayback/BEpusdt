package model

import (
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/core"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/utils"
)

const (
	OrderNotifyStateSucc = 1 // 回调成功
	OrderNotifyStateFail = 0 // 回调失败

	OrderStatusWaiting    = 1 // 等待支付
	OrderStatusSuccess    = 2 // 交易确认成功
	OrderStatusExpired    = 3 // 订单过期
	OrderStatusCanceled   = 4 // 订单取消
	OrderStatusConfirming = 5 // 等待交易确认
	OrderStatusFailed     = 6 // 交易确认失败

	BscBnb      TradeType = "bsc.bnb"
	EthereumEth TradeType = "ethereum.eth"
	TronTrx     TradeType = "tron.trx"
	TonGram     TradeType = "ton.gram"

	UsdtTrc20    TradeType = "usdt.trc20"
	UsdcTrc20    TradeType = "usdc.trc20"
	UsdtPolygon  TradeType = "usdt.polygon"
	UsdcPolygon  TradeType = "usdc.polygon"
	UsdtArbitrum TradeType = "usdt.arbitrum"
	UsdcArbitrum TradeType = "usdc.arbitrum"
	UsdtErc20    TradeType = "usdt.erc20"
	UsdcErc20    TradeType = "usdc.erc20"
	UsdtBep20    TradeType = "usdt.bep20"
	UsdcBep20    TradeType = "usdc.bep20"
	UsdtXlayer   TradeType = "usdt.xlayer"
	UsdcXlayer   TradeType = "usdc.xlayer"
	UsdcBase     TradeType = "usdc.base"
	UsdtSolana   TradeType = "usdt.solana"
	UsdcSolana   TradeType = "usdc.solana"
	UsdtAptos    TradeType = "usdt.aptos"
	UsdcAptos    TradeType = "usdc.aptos"
	UsdtPlasma   TradeType = "usdt.plasma"
	UsdtTon      TradeType = "usdt.ton"
)

const (
	OrderApiTypeEpusdt      = "epusdt"       // epusdt
	OrderApiTypeEpusdtOrder = "epusdt_order" // epusdt create-order
	OrderApiTypeEpay        = "epay"         // 彩虹易支付
	OrderApiTypeAdmin       = "admin"        // 管理后台
)

type Order struct {
	Id
	OrderId           string     `gorm:"column:order_id;type:varchar(128);not null;index;comment:商户ID" json:"order_id"`
	TradeId           string     `gorm:"column:trade_id;type:varchar(128);not null;uniqueIndex;comment:本地ID" json:"trade_id"`
	TradeType         TradeType  `gorm:"column:trade_type;type:varchar(20);not null;index;comment:交易类型" json:"trade_type"`
	Fiat              Fiat       `gorm:"column:fiat;type:varchar(16);not null;index;default:CNY;comment:法定货币" json:"fiat"`
	Crypto            Crypto     `gorm:"column:crypto;type:varchar(16);not null;index;default:USDT;comment:加密货币" json:"crypto"`
	CurrencyLimit     string     `gorm:"column:currency_limit;type:varchar(255);not null;default:'';comment:限定币种" json:"currency_limit"`
	Rate              string     `gorm:"column:rate;type:varchar(10);not null;comment:交易汇率" json:"rate"`
	Amount            string     `gorm:"column:amount;type:varchar(32);not null;default:0.00;comment:交易数额" json:"amount"`
	Money             string     `gorm:"column:money;type:varchar(32);not null;default:0.00;comment:交易金额" json:"money"`
	Address           string     `gorm:"column:address;type:varchar(128);index;not null;comment:收款地址" json:"address"`
	FromAddress       string     `gorm:"column:from_address;type:varchar(128);not null;default:'';comment:支付地址" json:"from_address"`
	MatchAddress      string     `gorm:"column:match_address;type:varchar(128);not null;default:'';comment:校验地址" json:"match_address"`
	AddressLocked     bool       `gorm:"column:address_locked;not null;default:false;comment:地址锁定 1:独占 0:共享" json:"address_locked"`
	Status            int        `gorm:"column:status;not null;default:1;index;index:idx_order_notify_retry,priority:1;comment:交易状态" json:"status"`
	Name              string     `gorm:"column:name;type:varchar(64);not null;default:'';comment:商品名称" json:"name"`
	ApiType           string     `gorm:"column:api_type;type:varchar(20);not null;default:'epusdt';comment:API类型" json:"api_type"`
	ReturnUrl         string     `gorm:"column:return_url;type:text;not null;default:'';comment:同步地址" json:"return_url"`
	NotifyUrl         string     `gorm:"column:notify_url;type:varchar(255);not null;default:'';comment:异步地址" json:"notify_url"`
	NotifyNum         int        `gorm:"column:notify_num;not null;default:0;index:idx_order_notify_retry,priority:3;comment:回调次数" json:"notify_num"`
	NotifyState       int        `gorm:"column:notify_state;not null;default:0;index:idx_order_notify_retry,priority:2;comment:回调状态 1：成功 0：失败" json:"notify_state"`
	RefHash           string     `gorm:"column:ref_hash;type:varchar(128);not null;default:'';index;comment:交易哈希" json:"ref_hash"`
	RefReceiptKey     string     `gorm:"column:ref_receipt_key;type:varchar(192);not null;default:'';index;comment:链上收据唯一键" json:"-"`
	RefBlockNum       int        `gorm:"column:ref_block_num;not null;default:0;comment:区块索引" json:"ref_block_num"`
	ExpiredAt         time.Time  `gorm:"column:expired_at;not null;comment:失效时间" json:"expired_at"`
	ConfirmedAt       *time.Time `gorm:"column:confirmed_at;not null;comment:交易确认时间" json:"confirmed_at"`
	ClientFingerprint string     `gorm:"column:client_fingerprint;type:varchar(64);not null;default:'';comment:'客户端指纹'" json:"-"`
	AutoTimeAt
}

type MethodItem struct {
	Amount          string `json:"amount"`
	ActualAmount    string `json:"actual_amount"`
	Fiat            string `json:"fiat"`
	ExchangeRate    string `json:"exchange_rate"`
	Currency        string `json:"currency"`
	Network         string `json:"network"`
	TokenNetName    string `json:"token_net_name"`
	TokenCustomName string `json:"token_custom_name"`
	IsPopular       bool   `json:"is_popular"`
}

func (o *Order) SetCanceled() error {
	core.New()
	o.Status = OrderStatusCanceled

	return Db.Save(o).Error
}

// CanReselectPayment 判断订单是否支持重选交易类型
func (o *Order) CanReselectPayment() bool {
	if o.Status != OrderStatusWaiting {
		return false
	}

	if o.ApiType != OrderApiTypeEpusdtOrder && o.ApiType != OrderApiTypeAdmin {
		return false
	}

	return true
}

func (o *Order) FingerprintBound() bool {
	return o.ClientFingerprint != ""
}

func (o *Order) MatchFingerprint(fingerprint string) bool {
	return !o.FingerprintBound() || o.ClientFingerprint == fingerprint
}

func (o *Order) SetExpired() {
	o.Status = OrderStatusExpired

	Db.Save(o)
}

func (o *Order) SetSuccess() {
	o.Status = OrderStatusSuccess

	Db.Save(o)
}

func (o *Order) SetFailed() {
	o.Status = OrderStatusFailed

	Db.Save(o)
}

func (o *Order) MarkConfirming(blockNum int, from, hash string, at time.Time, amount decimal.Decimal) error {
	return o.MarkConfirmingReceipt(blockNum, from, hash, "", at, amount)
}

var (
	ErrReceiptAlreadyClaimed = errors.New("blockchain receipt already claimed")
	ErrOrderStateConflict    = errors.New("order state changed")
)

// MarkConfirmingReceipt atomically claims an immutable chain receipt before
// an order can enter settlement. A partial unique database index protects the
// same receipt from concurrent claims by different orders.
func (o *Order) MarkConfirmingReceipt(blockNum int, from, hash, receiptKey string, at time.Time, amount decimal.Decimal) error {
	receiptKey = strings.TrimSpace(receiptKey)
	values := map[string]any{
		"from_address":    from,
		"confirmed_at":    at,
		"ref_hash":        hash,
		"ref_block_num":   blockNum,
		"ref_receipt_key": receiptKey,
		"status":          OrderStatusConfirming,
	}
	amountValue := o.Amount
	moneyValue := o.Money
	if o.AddressLocked {
		rate, _ := decimal.NewFromString(o.Rate)
		amountValue = amount.String()
		moneyValue = rate.Mul(amount).String()
		values["amount"] = amountValue
		values["money"] = moneyValue
	}

	query := Db.Model(&Order{}).
		Where("id = ? and status in (?)", o.ID, []int{OrderStatusWaiting, OrderStatusExpired, OrderStatusCanceled})
	if receiptKey != "" {
		query = query.Where("ref_receipt_key = ''")
	}
	result := query.Updates(values)
	if result.Error != nil {
		if receiptKey != "" {
			var count int64
			Db.Model(&Order{}).Where("ref_receipt_key = ? and id <> ?", receiptKey, o.ID).Count(&count)
			if count > 0 {
				return ErrReceiptAlreadyClaimed
			}
		}
		return result.Error
	}
	if result.RowsAffected == 1 {
		o.FromAddress = from
		o.ConfirmedAt = &at
		o.RefHash = hash
		o.RefReceiptKey = receiptKey
		o.RefBlockNum = blockNum
		o.Status = OrderStatusConfirming
		o.Amount = amountValue
		o.Money = moneyValue
		return nil
	}

	var current Order
	if err := Db.Where("id = ?", o.ID).First(&current).Error; err != nil {
		return err
	}
	if receiptKey != "" && current.RefReceiptKey == receiptKey &&
		(current.Status == OrderStatusConfirming || current.Status == OrderStatusSuccess) {
		*o = current
		return nil
	}
	return ErrOrderStateConflict
}

// ClaimConfirmingReceipt upgrades an in-flight order created by an older
// release so it can pass the same immutable receipt uniqueness guard.
func (o *Order) ClaimConfirmingReceipt(receiptKey string) error {
	receiptKey = strings.TrimSpace(receiptKey)
	if receiptKey == "" {
		return ErrReceiptAlreadyClaimed
	}
	result := Db.Model(&Order{}).
		Where("id = ? and status = ? and ref_hash = ? and ref_receipt_key = ''", o.ID, OrderStatusConfirming, o.RefHash).
		Update("ref_receipt_key", receiptKey)
	if result.Error != nil {
		var count int64
		Db.Model(&Order{}).Where("ref_receipt_key = ? and id <> ?", receiptKey, o.ID).Count(&count)
		if count > 0 {
			return ErrReceiptAlreadyClaimed
		}
		return result.Error
	}
	if result.RowsAffected == 1 {
		o.RefReceiptKey = receiptKey
		return nil
	}
	var current Order
	if err := Db.Where("id = ?", o.ID).First(&current).Error; err != nil {
		return err
	}
	if current.RefReceiptKey == receiptKey {
		*o = current
		return nil
	}
	return ErrOrderStateConflict
}

func (o *Order) SetNotifyState(state int) error {
	o.NotifyNum += 1
	o.NotifyState = state

	return Db.Save(o).Error
}

func (o *Order) GetStatusLabel() string {
	label := "🟢收款成功"
	if o.Status == OrderStatusExpired {
		label = "🔴交易过期"
	}
	if o.Status == OrderStatusWaiting {
		label = "🟡等待支付"
	}
	if o.Status == OrderStatusCanceled {
		label = "⚪️订单取消"
	}

	return label
}

func (o *Order) GetStatusEmoji() string {
	label := "🟢"
	if o.Status == OrderStatusExpired {
		label = "🔴"
	}
	if o.Status == OrderStatusWaiting {
		label = "🟡"
	}
	if o.Status == OrderStatusCanceled {
		label = "⚪️"
	}

	return label
}

func (o *Order) GetTxUrl() string {
	return GetTxUrl(o.TradeType, o.RefHash)
}

func (o *Order) RedirectUrl() string {
	var redirect = o.ReturnUrl
	if o.Status == OrderStatusSuccess && o.ApiType == OrderApiTypeEpay {
		redirect = fmt.Sprintf("%s?%s", redirect, o.BuildNotifyParams())
	}

	return redirect
}

func (o *Order) BuildNotifyParams() string {
	var signStr = utils.Md5String(fmt.Sprintf("money=%s&name=%s&out_trade_no=%s&pid=%s&trade_no=%s&trade_status=TRADE_SUCCESS&type=%s",
		cast.ToString(o.Money), o.Name, o.OrderId, conf.Pid, o.TradeId, o.TradeType) + AuthTokenForOrderID(o.OrderId))
	var params = fmt.Sprintf("money=%s&name=%s&out_trade_no=%s&pid=%s&trade_no=%s&trade_status=TRADE_SUCCESS&type=%s",
		cast.ToString(o.Money), url.QueryEscape(o.Name), url.QueryEscape(o.OrderId), conf.Pid, o.TradeId, o.TradeType)

	return fmt.Sprintf("%s&sign=%s", params, signStr)
}

func (o *Order) GetMethods(crypto Crypto) []MethodItem {
	var items = make([]MethodItem, 0)
	if o.Status != OrderStatusWaiting {
		return items
	}
	if o.TradeType != "" && !o.CanReselectPayment() {
		return items
	}

	allTrades := GetAllTradeConfig()

	// 解析限定币种
	var whitelist = make(map[string]bool)
	var blacklist = make(map[string]bool)
	if o.CurrencyLimit != "" {
		for _, c := range strings.Split(o.CurrencyLimit, ",") {
			c = strings.TrimSpace(c)
			if strings.HasPrefix(c, "-") {
				blacklist[strings.ToUpper(strings.TrimPrefix(c, "-"))] = true
			} else {
				whitelist[strings.ToUpper(c)] = true
			}
		}
	}

	for tradeTypeStr, typeConf := range allTrades {
		// 如果指定了货币，则进行过滤
		if crypto != "" && typeConf.Crypto != crypto {
			continue
		}

		// Check blacklist
		if len(blacklist) > 0 && blacklist[string(typeConf.Crypto)] {
			continue
		}

		// Check whitelist
		if len(whitelist) > 0 && !whitelist[string(typeConf.Crypto)] {
			continue
		}

		// 检查是否有可用钱包
		count := len(GetAvailableWallets(TradeType(tradeTypeStr)))
		if count == 0 {
			continue
		}

		// 获取汇率配置的浮动语法
		syntax := GetK(ConfKey(fmt.Sprintf("rate_float_%s_%s", typeConf.Crypto, o.Fiat)))

		// 获取汇率
		rate, err := GetOrderRate(typeConf.Crypto, o.Fiat, syntax)
		if err != nil {
			log.Error(fmt.Sprintf("GetPaymentMethods: get order rate error: %s", err.Error()))
			continue
		}

		// 计算实际支付金额 (加密货币)
		// Money 是法币金额
		moneyDecimal, _ := decimal.NewFromString(o.Money)

		// 计算精度
		atom, precision := GetAtomicity(TradeType(tradeTypeStr))
		actualAmount := moneyDecimal.DivRound(rate, precision)
		if actualAmount.LessThan(atom) {
			actualAmount = atom
		}

		items = append(items, MethodItem{
			Amount:          o.Money,
			ActualAmount:    actualAmount.String(),
			Fiat:            string(o.Fiat),
			ExchangeRate:    rate.String(),
			Currency:        string(typeConf.Crypto),
			Network:         string(typeConf.Network),
			TokenNetName:    typeConf.NetworkName,
			TokenCustomName: "",    // 暂为空
			IsPopular:       false, // 暂为 false
		})
	}

	// Sort by Currency A-Z
	sort.Slice(items, func(i, j int) bool {
		if items[i].Currency != items[j].Currency {
			return items[i].Currency < items[j].Currency
		}
		return items[i].Network < items[j].Network
	})

	return items
}

func (o *Order) Network() any {
	type network struct {
		Alias   string  `json:"alias"`
		Name    string  `json:"name"`
		Crypto  Crypto  `json:"crypto"`
		Network Network `json:"network"`
	}

	var net = network{}
	info, ok := registry[o.TradeType]
	if !ok {
		return net
	}

	net.Name = info.NetworkName
	net.Alias = info.Alias
	net.Crypto = info.Crypto
	net.Network = info.Network

	return net
}

func (o *Order) TableName() string {
	return "bep_order"
}

func GetTradeOrder(tradeId string) (Order, bool) {
	var order Order
	res := Db.Where("trade_id = ?", tradeId).Limit(1).Find(&order)

	return order, res.RowsAffected > 0
}

func GetOrderByStatus(Status int) []Order {
	orders := make([]Order, 0)

	Db.Where("status = ?", Status).Find(&orders)

	return orders
}

func GetNotifyFailedTradeOrders() ([]Order, error) {
	var orders []Order
	maxRetry := cast.ToInt(GetC(NotifyMaxRetry))
	if maxRetry <= 0 {
		maxRetry = cast.ToInt(defaultConf[NotifyMaxRetry])
	}

	res := Db.Where("status = ?", OrderStatusSuccess).
		Where("notify_state = ?", OrderNotifyStateFail).
		Where("notify_num <= ?", maxRetry).Find(&orders)

	return orders, res.Error
}

// CalcTradeAmount 计算当前实际可用的交易金额
func CalcTradeAmount(wallets []Wallet, rate decimal.Decimal, p OrderParams) (Wallet, string, error) {
	if p.AddressLocked {
		return LockTradeAddress(wallets, p.TradeType)
	}

	var orders []Order
	lock := make(map[string]bool)
	status := []int{OrderStatusConfirming, OrderStatusWaiting}
	Db.Where("status in (?) and trade_type = ?", status, p.TradeType).Find(&orders)
	for _, order := range orders {
		matchAddress := order.MatchAddress
		if matchAddress == "" {
			matchAddress = order.Address
			if !AddrCaseSens(order.TradeType) {
				matchAddress = strings.ToLower(matchAddress)
			}
		}
		lock[paymentAllocationKey(matchAddress, order.TradeType, order.Amount)] = true
	}

	atom, precision := GetAtomicity(p.TradeType)
	if rate.LessThanOrEqual(decimal.Zero) || precision <= 0 {
		return Wallet{}, "", errors.New(fmt.Sprintf("[%v - %v]原子颗粒度计算异常，联系管理员处理！", atom, precision))
	}

	amount := p.Money.DivRound(rate, precision)
	if amount.LessThan(atom) { // 低于最小原子精度，从最小原子精度开始计算
		amount = atom
	}
	if UniqueAmountEnabled(p.TradeType) {
		return allocateUniqueTradeAmount(wallets, amount, precision, p.TradeType, lock)
	}

	var i = 0
	var m = 100
	for {
		for _, w := range wallets {
			k := paymentAllocationKey(w.GetMatchAddr(), p.TradeType, amount.String())
			if _, ok := lock[k]; ok {
				continue
			}

			return w, amount.String(), nil
		}

		// 已经被占用，每次递增一个原子精度
		amount = amount.Add(atom)
		if i++; i > m {
			return Wallet{}, "", errors.New("计算交易金额异常，联系管理员处理！")
		}
	}
}

const uniquePaymentPrecision int32 = 5

// allocateUniqueTradeAmount keeps the normal rounded amount as the base and
// uses the remaining decimal positions as an order identifier. For the usual
// two-decimal stablecoin base this yields 001-999 at decimal positions 3-5.
func allocateUniqueTradeAmount(wallets []Wallet, base decimal.Decimal, basePrecision int32, tradeType TradeType, lock map[string]bool) (Wallet, string, error) {
	if basePrecision < 0 || basePrecision >= uniquePaymentPrecision {
		return Wallet{}, "", fmt.Errorf("unique payment amount requires fewer than %d base decimals", uniquePaymentPrecision)
	}

	suffixCount := int64(1)
	for i := basePrecision; i < uniquePaymentPrecision; i++ {
		suffixCount *= 10
	}
	suffixCount--
	unit := decimal.New(1, -uniquePaymentPrecision)
	for _, suffix := range uniqueSuffixSequence(suffixCount) {
		amount := base.Add(unit.Mul(decimal.NewFromInt(suffix))).StringFixed(uniquePaymentPrecision)
		for _, wallet := range wallets {
			key := paymentAllocationKey(wallet.GetMatchAddr(), tradeType, amount)
			if !lock[key] {
				return wallet, amount, nil
			}
		}
	}

	return Wallet{}, "", errors.New("unique payment amount space exhausted; create a new order or add a receiving wallet")
}

func uniqueSuffixSequence(max int64) []int64 {
	if max <= 0 {
		return nil
	}
	start, ok := secureRandomInt(max)
	if !ok {
		start = 0
	}
	step := int64(1)
	for attempt := 0; ok && attempt < 16 && max > 1; attempt++ {
		candidate, randomOK := secureRandomInt(max - 1)
		if !randomOK {
			break
		}
		candidate++
		if greatestCommonDivisor(candidate, max) == 1 {
			step = candidate
			break
		}
	}

	result := make([]int64, max)
	for i := int64(0); i < max; i++ {
		result[i] = (start+i*step)%max + 1
	}
	return result
}

func secureRandomInt(max int64) (int64, bool) {
	value, err := cryptorand.Int(cryptorand.Reader, big.NewInt(max))
	if err != nil {
		return 0, false
	}
	return value.Int64(), true
}

func greatestCommonDivisor(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func paymentAllocationKey(address string, tradeType TradeType, amount string) string {
	address = strings.TrimSpace(address)
	if !AddrCaseSens(tradeType) {
		address = strings.ToLower(address)
	}
	if parsed, err := decimal.NewFromString(strings.TrimSpace(amount)); err == nil {
		amount = parsed.String()
	}
	return address + "\x00" + amount
}

// LockTradeAddress 检测交易地址，独占使用
func LockTradeAddress(wallets []Wallet, t TradeType) (Wallet, string, error) {
	zero := decimal.Zero.String()
	status := []int{OrderStatusConfirming, OrderStatusWaiting}
	for _, w := range wallets {
		var o Order
		Db.Where("match_address = ? and status in (?) and trade_type = ? and address_locked = ?", w.GetMatchAddr(), status, t, true).Order("id desc").Limit(1).Find(&o)
		if o.ID == 0 {
			return w, zero, nil
		}
	}

	return Wallet{}, zero, errors.New("暂无可用钱包地址")
}

// CalcTradeExpiredAt 计算订单过期时间 最小180，最大3600，默认1200
func CalcTradeExpiredAt(sec int64) time.Time {
	if sec >= 180 && sec <= 3600 {
		return time.Now().Add(time.Duration(sec) * time.Second)
	}

	return time.Now().Add(time.Duration(cast.ToUint64(GetK(PaymentTimeout))) * time.Second)
}

func GetAtomicity(t TradeType) (decimal.Decimal, int32) {
	confKey, ok := GetTradeAtomKey(t)
	if !ok {
		confKey = "atom_usdt"
	}

	atom, _ := decimal.NewFromString(GetK(confKey))

	return atom, cast.ToInt32(math.Abs(float64(atom.Exponent())))
}
