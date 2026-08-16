package task

import (
	"context"
	"time"

	"github.com/smallnest/chanx"
	"github.com/v03413/bepusdt/app/conf"
	"github.com/v03413/bepusdt/app/model"
	"github.com/v03413/bepusdt/app/utils"
)

func polygonInit() {
	ctx := context.Background()
	pol := evm{
		Network: conf.Polygon,
		Block: block{
			ConfirmedOffset: 40,
		},
		Client:         utils.NewHttpClient(),
		blockScanQueue: chanx.NewUnboundedChan[evmBlock](ctx, 30),
		RPCEndpoints:   []string{model.GetC(model.RpcEndpointPolygon), model.GetC(model.RpcEndpointPolygonFallback)},
	}
	registerEVMVerifier(&pol)

	Register(Task{Callback: pol.blockDispatch})
	Register(Task{Callback: pol.syncBlocksForward, Duration: time.Second * 5})
	Register(Task{Callback: pol.tradeConfirmHandle, Duration: time.Second * 5})
	Register(Task{Callback: pol.lookbackBlocks, Duration: time.Second * 15})
}
