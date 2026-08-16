package task

import (
	"context"
	"time"

	"github.com/smallnest/chanx"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/utils"
)

func bscInit() {
	ctx := context.Background()
	bsc := evm{
		Network: conf.Bsc,
		Block: block{
			ConfirmedOffset: 15,
		},
		Native: evmNative{
			Parse:     true,
			Decimal:   conf.BscBnbDecimals,
			TradeType: model.BscBnb,
		},
		Client:         utils.NewHttpClient(),
		blockScanQueue: chanx.NewUnboundedChan[evmBlock](ctx, 30),
		RPCEndpoints:   []string{model.GetC(model.RpcEndpointBsc), model.GetC(model.RpcEndpointBscFallback)},
	}
	registerEVMVerifier(&bsc)

	Register(Task{Callback: bsc.blockDispatch})
	Register(Task{Callback: bsc.syncBlocksForward, Duration: time.Second * 5})
	Register(Task{Callback: bsc.tradeConfirmHandle, Duration: time.Second * 5})
	Register(Task{Callback: bsc.lookbackBlocks, Duration: time.Second * 15})
}
