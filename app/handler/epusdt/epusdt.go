package epusdt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"github.com/v03413/bepusdt/app/log"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/task"
	"github.com/v03413/bepusdt/app/utils"
)

type Epusdt struct{}

type createReq struct {
	OrderID     string     `json:"order_id" binding:"required"`
	NotifyURL   string     `json:"notify_url" binding:"required"`
	RedirectURL string     `json:"redirect_url" binding:"required"`
	Signature   string     `json:"signature" binding:"required"`
	Amount      float64    `json:"amount"`
	Name        string     `json:"name"`
	Fiat        model.Fiat `json:"fiat"`
	TradeType   string     `json:"trade_type"`
	Address     string     `json:"address"`
	Timeout     int64      `json:"timeout"`
	Rate        string     `json:"rate"`
}

type createOrderReq struct {
	OrderID     string     `json:"order_id" binding:"required"`
	NotifyURL   string     `json:"notify_url" binding:"required"`
	RedirectURL string     `json:"redirect_url" binding:"required"`
	Signature   string     `json:"signature" binding:"required"`
	Amount      float64    `json:"amount"`
	Name        string     `json:"name"`
	Fiat        model.Fiat `json:"fiat"`
	Currencies  string     `json:"currencies"`
	Timeout     int64      `json:"timeout"`
	Locale      string     `json:"locale"`
}

type updateOrderReq struct {
	TradeID  string `json:"trade_id" binding:"required"`
	Currency string `json:"currency" binding:"required"`
	Network  string `json:"network" binding:"required"`
	Locale   string `json:"locale"`
}

type cancelReq struct {
	TradeID   string `json:"trade_id" binding:"required"`
	Signature string `json:"signature" binding:"required"`
}

type infoReq struct {
	TradeID string `json:"trade_id" binding:"required"`
}

type methodsReq struct {
	TradeID  string `json:"trade_id" binding:"required"`
	Currency string `json:"currency"`
}

type submitTransactionReq struct {
	TradeID         string `json:"trade_id" binding:"required"`
	TransactionHash string `json:"transaction_hash" binding:"required"`
}

var hashSubmitAttempts sync.Map

const hashSubmitThrottle = 5 * time.Second

func loadPayOrder(ctx *gin.Context, tradeID string) (model.Order, bool) {
	order, ok := model.GetTradeOrder(tradeID)
	if !ok {
		ctx.JSON(200, respFailJson("order not found"))
		return model.Order{}, false
	}

	if order.FingerprintBound() && !order.MatchFingerprint(utils.ClientFingerprint(ctx)) {
		ctx.JSON(200, respFailJson("order not found"))
		return model.Order{}, false
	}

	return order, true
}

func (Epusdt) Notify(ctx *gin.Context) {
	rawData, err := ctx.GetRawData()
	if err != nil {
		ctx.String(200, "fail")
		return
	}

	m := make(map[string]any)
	if err = json.Unmarshal(rawData, &m); err != nil {
		ctx.String(200, "fail")
		return
	}

	sign, ok := m["signature"]
	if !ok {
		ctx.String(200, "fail")
		return
	}

	if utils.EpusdtSign(m, authTokenForPayload(m)) != sign {
		ctx.String(200, "fail")
		return
	}

	ctx.String(200, "ok")
}

func (Epusdt) CreateOrder(ctx *gin.Context) {
	var req createOrderReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("CreateOrder: request error: %s", err.Error())))

		return
	}

	if !utils.IsAllowedCallbackURL(req.NotifyURL) {
		ctx.JSON(200, respFailJson("notify_url 地址不合法"))

		return
	}
	if !utils.IsAllowedCallbackURL(req.RedirectURL) {
		ctx.JSON(200, respFailJson("redirect_url 地址不合法"))

		return
	}

	// Preserve the public scheme when TLS is terminated by the reverse proxy.
	host := utils.GetRequestHost(ctx.Request)

	if req.Fiat == "" {
		req.Fiat = model.CNY
	}
	if err := validateCreateOrderRates(req.Currencies, req.Fiat); err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("CreateOrder: %s", err.Error())))
		return
	}

	// 创建待付款订单
	order, err := model.BuildPendingOrder(model.OrderParams{
		Money:         decimal.NewFromFloat(req.Amount),
		ApiType:       model.OrderApiTypeEpusdtOrder,
		OrderId:       req.OrderID,
		RedirectUrl:   req.RedirectURL,
		NotifyUrl:     req.NotifyURL,
		Name:          req.Name,
		Timeout:       req.Timeout,
		Fiat:          req.Fiat,
		CurrencyLimit: req.Currencies,
	})
	if err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("CreateOrder: order create failed: %s", err.Error())))
		return
	}

	log.Info(fmt.Sprintf("订单创建成功 商户订单：%s", req.OrderID))

	// 返回响应数据
	ctx.JSON(200, respSuccJson(gin.H{
		"fiat":            order.Fiat,
		"trade_id":        order.TradeId,
		"order_id":        order.OrderId,
		"name":            order.Name,
		"status":          order.Status,
		"amount":          order.Money,
		"expiration_time": uint64(order.ExpiredAt.Sub(time.Now()).Seconds()),
		"payment_url":     model.CheckoutURLWithLocale(host, order.TradeId, req.Locale),
		"network":         order.GetMethods(""),
		"reselect":        order.CanReselectPayment(),
	}))
}

func (Epusdt) UpdateOrder(ctx *gin.Context) {
	// 接收 trade_id, currency, network 三个参数
	var req updateOrderReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("UpdateOrder: request error: %s", err.Error())))
		return
	}

	// Preserve the public scheme when TLS is terminated by the reverse proxy.
	host := utils.GetRequestHost(ctx.Request)

	order, ok := loadPayOrder(ctx, req.TradeID)
	if !ok {
		return
	}

	// 仅待付订单才可更新订单
	if order.Status != model.OrderStatusWaiting {
		ctx.JSON(200, respFailJson("update order failed: order status does not allow payment updates"))
		return
	}

	if order.TradeType != "" && !order.CanReselectPayment() {
		ctx.JSON(200, respFailJson("update order failed: order status does not allow payment updates"))
		return
	}

	fp := utils.ClientFingerprint(ctx)

	// 拒绝过期订单
	remaining := time.Until(order.ExpiredAt)
	if remaining <= 0 {
		ctx.JSON(200, respFailJson("update order failed: order expired"))
		return
	}

	// 根据 currency 和 network 解析出 TradeType
	tradeType, err := model.GetTradeTypeByCurrencyAndNetwork(req.Currency, req.Network)
	if err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("UpdateOrder: unsupported payment method: %s - %s", req.Currency, req.Network)))
		return
	}

	// 重建订单（更新支付方式）
	// 注意：RebuildOrder 需要 OrderParams，我们需要从现有订单构造参数
	money, _ := decimal.NewFromString(order.Money)
	params := model.OrderParams{
		Money:             money,
		OrderId:           order.OrderId,
		TradeType:         tradeType, // 新的交易类型
		RedirectUrl:       order.ReturnUrl,
		NotifyUrl:         order.NotifyUrl,
		Name:              order.Name,
		Timeout:           int64(math.Ceil(remaining.Seconds())),
		Fiat:              order.Fiat,
		ClientFingerprint: fp,
	}

	newOrder, err := model.RebuildOrder(order, params)
	if err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("update order failed: %s", err.Error())))
		return
	}

	// 返回响应数据
	ctx.JSON(200, respSuccJson(gin.H{
		"fiat":            newOrder.Fiat,
		"trade_type":      newOrder.TradeType,
		"trade_id":        newOrder.TradeId,
		"order_id":        newOrder.OrderId,
		"status":          newOrder.Status,
		"amount":          newOrder.Money,
		"actual_amount":   newOrder.Amount,
		"token":           newOrder.Address,
		"expiration_time": uint64(newOrder.ExpiredAt.Sub(time.Now()).Seconds()),
		"payment_url":     model.CheckoutURLWithLocale(host, newOrder.TradeId, req.Locale),
	}))
}

func (Epusdt) CreateTransaction(ctx *gin.Context) {
	var req createReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("请求参数错误：%s", err.Error())))

		return
	}

	if !utils.IsAllowedCallbackURL(req.NotifyURL) {
		ctx.JSON(200, respFailJson("notify_url 地址不合法"))

		return
	}
	if !utils.IsAllowedCallbackURL(req.RedirectURL) {
		ctx.JSON(200, respFailJson("redirect_url 地址不合法"))

		return
	}

	if req.Fiat == "" {
		req.Fiat = model.CNY
	}
	if req.TradeType == "" {
		req.TradeType = string(model.UsdtTrc20)
	}

	order, err := model.StartBuildOrder(model.OrderParams{
		Money:         decimal.NewFromFloat(req.Amount),
		ApiType:       model.OrderApiTypeEpusdt,
		Address:       req.Address,
		AddressLocked: req.Amount == 0,
		OrderId:       req.OrderID,
		TradeType:     model.TradeType(req.TradeType),
		RedirectUrl:   req.RedirectURL,
		NotifyUrl:     req.NotifyURL,
		Name:          req.Name,
		Timeout:       req.Timeout,
		Rate:          req.Rate,
		Fiat:          req.Fiat,
	})
	if err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("订单创建失败：%s", err.Error())))

		return
	}

	log.Info(fmt.Sprintf("订单创建成功 商户订单：%s", req.OrderID))

	// 返回响应数据
	ctx.JSON(200, respSuccJson(gin.H{
		"fiat":            order.Fiat,
		"trade_type":      order.TradeType,
		"trade_id":        order.TradeId,
		"order_id":        order.OrderId,
		"status":          order.Status,
		"amount":          order.Money,
		"actual_amount":   order.Amount,
		"token":           order.Address,
		"expiration_time": uint64(order.ExpiredAt.Sub(time.Now()).Seconds()),
		"payment_url":     model.CheckoutUrl(utils.GetRequestHost(ctx.Request), order.TradeId),
	}))
}

func (Epusdt) CancelTransaction(ctx *gin.Context) {
	var req cancelReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("请求参数错误：%s", err.Error())))

		return
	}

	order, ok := model.GetTradeOrder(req.TradeID)
	if !ok {
		ctx.JSON(200, respFailJson("订单不存在"))

		return
	}

	if order.Status != model.OrderStatusWaiting {
		ctx.JSON(200, respFailJson(fmt.Sprintf("当前订单(%s)状态不允许取消", req.TradeID)))

		return
	}

	if err := order.SetCanceled(); err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("订单取消失败：%s", err.Error())))

		return
	}

	ctx.JSON(200, respSuccJson(gin.H{"trade_id": req.TradeID}))
}

// QueryTransaction exposes merchant-side reconciliation without using the
// browser checkout endpoint, whose fingerprint binding is intentionally kept.
func (Epusdt) QueryTransaction(ctx *gin.Context) {
	var req infoReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("请求参数错误：%s", err.Error())))
		return
	}

	order, ok := model.GetTradeOrder(req.TradeID)
	if !ok {
		ctx.JSON(200, respFailJson("订单不存在"))
		return
	}

	data := gin.H{
		"trade_id":             order.TradeId,
		"order_id":             order.OrderId,
		"trade_type":           order.TradeType,
		"fiat":                 order.Fiat,
		"currency":             order.Crypto,
		"amount":               order.Money,
		"actual_amount":        order.Amount,
		"status":               order.Status,
		"block_transaction_id": order.RefHash,
		"expired_at":           order.ExpiredAt.UTC().Format(time.RFC3339),
	}
	if order.CreatedAt != nil {
		data["created_at"] = order.CreatedAt.Time().UTC().Format(time.RFC3339)
	}
	if order.ConfirmedAt != nil {
		data["confirmed_at"] = order.ConfirmedAt.UTC().Format(time.RFC3339)
	}

	ctx.JSON(200, respSuccJson(data))
}

func validateCreateOrderRates(currencies string, fiat model.Fiat) error {
	currencies = strings.TrimSpace(currencies)
	if currencies == "" || strings.HasPrefix(currencies, "-") {
		return nil
	}

	supported := model.GetSupportCrypto()
	for _, raw := range strings.Split(currencies, ",") {
		crypto := model.Crypto(strings.ToUpper(strings.TrimSpace(raw)))
		if crypto == "" {
			continue
		}
		if _, ok := supported[crypto]; !ok {
			return fmt.Errorf("unsupported currency: %s", crypto)
		}
		syntax := model.GetK(model.ConfKey(fmt.Sprintf("rate_float_%s_%s", crypto, fiat)))
		if _, err := model.GetOrderRate(crypto, fiat, syntax); err != nil {
			return err
		}
	}
	return nil
}

func (Epusdt) Checkout(ctx *gin.Context) {
	tradeId := ctx.Param("trade_id")
	if _, ok := model.GetTradeOrder(tradeId); !ok {
		ctx.String(200, "order not found")

		return
	}

	// 收银台模板
	tmpl := model.GetC(model.PaymentCheckout) + "/checkout.html"

	ctx.HTML(200, tmpl, gin.H{"trade_id": tradeId})
}

func (Epusdt) GetMethods(ctx *gin.Context) {
	var req methodsReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("request error: %s", err.Error())))
		return
	}

	order, ok := loadPayOrder(ctx, req.TradeID)
	if !ok {
		return
	}

	if order.Status != model.OrderStatusWaiting {
		ctx.JSON(200, respFailJson("error: Invalid order status"))
		return
	}

	if order.TradeType != "" && !order.CanReselectPayment() {
		ctx.JSON(200, respFailJson("error: The order status does not allow retrieving the payment method"))
		return
	}

	ctx.JSON(200, respSuccJson(gin.H{
		"methods":      order.GetMethods(model.Crypto(req.Currency)),
		"network_sort": model.GetC(model.PaymentNetworkSort),
	}))
}

func (Epusdt) Info(ctx *gin.Context) {
	var req infoReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("参数错误：%s", err.Error())))
		return
	}

	order, ok := loadPayOrder(ctx, req.TradeID)
	if !ok {
		return
	}
	hashSubmissionAllowed := order.FingerprintBound() && model.HashSubmissionEnabled(order.TradeType) &&
		(order.Status == model.OrderStatusWaiting ||
			(order.Status == model.OrderStatusExpired && model.GetHashSubmitLateWindow() > 0 &&
				time.Now().Before(order.ExpiredAt.Add(model.GetHashSubmitLateWindow()))))

	ctx.JSON(200, respSuccJson(gin.H{
		"network":                 order.Network(),                     // 网络信息
		"trade_id":                order.TradeId,                       // 交易编号
		"order_id":                order.OrderId,                       // 商户订单
		"trade_type":              order.TradeType,                     // 交易类型
		"status":                  order.Status,                        // 订单状态
		"money":                   order.Money,                         // 订单金额
		"actual_amount":           order.Amount,                        // 实付数额
		"token":                   order.Address,                       // 收款地址
		"fiat":                    order.Fiat,                          // 法币类型
		"name":                    order.Name,                          // 商品名称
		"expired_at":              order.ExpiredAt.Unix(),              // 截止时间
		"created_at":              order.CreatedAt.Time().Unix(),       // 创建时间
		"trade_url":               order.GetTxUrl(),                    // 链上详情
		"support_url":             model.GetC(model.PaymentSupportUrl), // 客服链接
		"redirect_url":            order.RedirectUrl(),                 // 跳转地址
		"reselect":                order.CanReselectPayment(),          // 是否允许确认交易类型后重选
		"hash_submission_enabled": model.HashSubmissionEnabled(order.TradeType),
		"hash_submission_allowed": hashSubmissionAllowed,
	}))
}

func (Epusdt) SubmitTransaction(ctx *gin.Context) {
	var req submitTransactionReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(200, respFailJson("invalid transaction submission"))
		return
	}
	order, ok := loadPayOrder(ctx, req.TradeID)
	if !ok {
		return
	}
	if !order.FingerprintBound() || !model.HashSubmissionEnabled(order.TradeType) {
		ctx.JSON(200, respFailJson("transaction submission unavailable"))
		return
	}
	if order.Status != model.OrderStatusWaiting && order.Status != model.OrderStatusExpired &&
		order.Status != model.OrderStatusConfirming && order.Status != model.OrderStatusSuccess {
		ctx.JSON(200, respFailJson("order status does not allow transaction submission"))
		return
	}
	now := time.Now()
	if now.After(order.ExpiredAt) && order.Status != model.OrderStatusConfirming && order.Status != model.OrderStatusSuccess {
		lateWindow := model.GetHashSubmitLateWindow()
		if lateWindow <= 0 || now.After(order.ExpiredAt.Add(lateWindow)) {
			ctx.JSON(200, respFailJson("order expired"))
			return
		}
	}
	hash, valid := task.NormalizeEVMHash(req.TransactionHash)
	if !valid {
		ctx.JSON(200, respFailJson("invalid transaction hash"))
		return
	}
	throttleKey := order.TradeId + ":" + utils.ClientFingerprint(ctx)
	if order.Status != model.OrderStatusConfirming && order.Status != model.OrderStatusSuccess {
		if last, found := hashSubmitAttempts.Load(throttleKey); found && now.Sub(last.(time.Time)) < hashSubmitThrottle {
			ctx.JSON(200, respSuccJson(gin.H{
				"verification_state": task.SubmittedTransactionPending,
				"order_status":       order.Status,
				"retry_after":        int(hashSubmitThrottle.Seconds()),
			}))
			return
		}
		hashSubmitAttempts.Store(throttleKey, now)
	}

	verifyCtx, cancel := context.WithTimeout(ctx.Request.Context(), 20*time.Second)
	defer cancel()
	result, err := task.SubmitEVMTransaction(verifyCtx, order, hash)
	if err != nil {
		switch {
		case errors.Is(err, task.ErrSubmittedTransactionInvalid):
			ctx.JSON(200, respFailJson("transaction does not match this order"))
		case errors.Is(err, model.ErrReceiptAlreadyClaimed):
			ctx.JSON(200, respFailJson("transaction receipt already used"))
		default:
			ctx.JSON(200, respFailJson("transaction verification unavailable"))
		}
		return
	}
	ctx.JSON(200, respSuccJson(gin.H{
		"verification_state":     result.State,
		"order_status":           result.OrderStatus,
		"confirmations":          result.Confirmations,
		"required_confirmations": result.RequiredConfirmations,
	}))
}

func (Epusdt) SignVerify(ctx *gin.Context) {
	rawData, err := ctx.GetRawData()
	if err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("json 数据读取错误 %s", err.Error())))
		ctx.Abort()

		return
	}

	m := make(map[string]any)
	if err = json.Unmarshal(rawData, &m); err != nil {
		ctx.JSON(200, respFailJson(fmt.Sprintf("json 数据解析错误 %s", err.Error())))
		ctx.Abort()

		return
	}

	sign, ok := m["signature"]
	if !ok {
		ctx.JSON(200, respFailJson("签名丢失"))
		ctx.Abort()

		return
	}

	authToken, ok := requestAuthToken(m)
	if !ok {
		ctx.JSON(200, respFailJson("订单不存在"))
		ctx.Abort()
		return
	}

	if utils.EpusdtSign(m, authToken) != sign {
		ctx.JSON(200, respFailJson("签名错误"))
		ctx.Abort()

		return
	}

	ctx.Request.Body = io.NopCloser(bytes.NewBuffer(rawData)) // 回写数据
	ctx.Next()
}

func requestAuthToken(payload map[string]any) (string, bool) {
	// trade_id takes precedence so an attacker cannot add a foreign order_id
	// field to authorize a query or cancellation with another merchant's token.
	if tradeID := payloadString(payload, "trade_id"); tradeID != "" {
		order, ok := model.GetTradeOrder(tradeID)
		if !ok {
			return "", false
		}
		return model.AuthTokenForOrderID(order.OrderId), true
	}
	return model.AuthTokenForOrderID(payloadString(payload, "order_id")), true
}

func authTokenForPayload(payload map[string]any) string {
	if tradeID := payloadString(payload, "trade_id"); tradeID != "" {
		if order, ok := model.GetTradeOrder(tradeID); ok {
			return model.AuthTokenForOrderID(order.OrderId)
		}
	}
	if orderID := payloadString(payload, "order_id"); orderID != "" {
		return model.AuthTokenForOrderID(orderID)
	}
	return model.AuthToken()
}

func payloadString(payload map[string]any, key string) string {
	value, ok := payload[key].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func respFailJson(message string) gin.H {

	return gin.H{"status_code": 400, "message": message}
}

func respSuccJson(data interface{}) gin.H {
	return gin.H{"status_code": 200, "message": "success", "data": data, "request_id": ""}
}
