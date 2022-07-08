package main

import (
	"github.com/tuingking/coinbit/deposit"
	"github.com/tuingking/coinbit/service"
)

func main() {
	service.Run([]string{"localhost:9092"}, deposit.DepositStream)
}
