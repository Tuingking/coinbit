package abovethreshold

import (
	"context"
	"encoding/json"
	"time"

	"github.com/lovoo/goka"
	"github.com/tuingking/coinbit/deposit"
	"github.com/tuingking/coinbit/topicinit"
)

const maxThreshold float64 = 10_000

var (
	group goka.Group = "aboveThreshold"
	Table goka.Table = goka.GroupTable(group)
)

type AboveThreshold struct {
	TotalAddition float64
	FirstDeposit  time.Time
	IsAbove       bool
}

type AboveThresholdCodec struct{}

func (c *AboveThresholdCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *AboveThresholdCodec) Decode(data []byte) (interface{}, error) {
	var m AboveThreshold
	return &m, json.Unmarshal(data, &m)
}

func checkThreshold(ctx goka.Context, msg interface{}) {
	var th *AboveThreshold
	if v := ctx.Value(); v == nil {
		th = new(AboveThreshold)
		th.FirstDeposit = time.Now()
	} else {
		th = v.(*AboveThreshold)
	}

	if v, ok := msg.(*deposit.Deposit); ok {
		th.TotalAddition += v.Amount

		if time.Since(th.FirstDeposit) > 2*time.Minute {
			th.TotalAddition = 0
			th.FirstDeposit = time.Now()
			th.IsAbove = false
		} else if th.TotalAddition > maxThreshold {
			th.IsAbove = true
		}
	}

	ctx.SetValue(th)
}

func PrepareTopics(brokers []string) {
	topicinit.EnsureStreamExists(string(deposit.DepositStream), brokers)
}

func Run(ctx context.Context, brokers []string) func() error {
	return func() error {
		tmc := goka.NewTopicManagerConfig()
		tmc.Table.Replication = 1
		tmc.Stream.Replication = 1

		g := goka.DefineGroup(group,
			goka.Input(deposit.DepositStream, new(deposit.DepositCodec), checkThreshold),
			goka.Persist(new(AboveThresholdCodec)),
		)
		p, err := goka.NewProcessor(brokers,
			g,
			goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
			goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
		)
		if err != nil {
			return err
		}
		return p.Run(ctx)
	}
}
