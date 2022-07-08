package balance

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lovoo/goka"
	"github.com/tuingking/coinbit/deposit"
	"github.com/tuingking/coinbit/topicinit"
)

var (
	group goka.Group = "balance.Group"
	Table goka.Table = goka.GroupTable(group)
)

type Balance struct {
	Balance float64 `json:"balance"`
}

type BalanceCodec struct{}

func (c *BalanceCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *BalanceCodec) Decode(data []byte) (interface{}, error) {
	var m Balance
	return &m, json.Unmarshal(data, &m)
}

func addBalance(ctx goka.Context, msg interface{}) {
	var b *Balance
	if v := ctx.Value(); v != nil {
		b = v.(*Balance)
	} else {
		b = new(Balance)
	}

	if v, ok := msg.(*deposit.Deposit); ok {
		b.Balance += v.Amount
	}

	ctx.SetValue(b)
	fmt.Printf("[proc] key: %s balance: %v, msg: %v\n", ctx.Key(), b.Balance, msg)
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
			goka.Input(deposit.DepositStream, new(deposit.DepositCodec), addBalance),
			goka.Persist(new(BalanceCodec)),
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
