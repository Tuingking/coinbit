package deposit

import (
	"fmt"

	"github.com/lovoo/goka"
	"google.golang.org/protobuf/proto"
)

var (
	DepositStream goka.Stream = "deposits"
)

type DepositCodec struct{}

func (jc *DepositCodec) Encode(value interface{}) ([]byte, error) {
	if v, ok := value.(*Deposit); ok {
		return proto.Marshal(v)
	}
	return nil, fmt.Errorf("codec requires value *Deposit, got %T", value)
}

func (jc *DepositCodec) Decode(data []byte) (interface{}, error) {
	var m Deposit
	return &m, proto.Unmarshal(data, &m)
}
