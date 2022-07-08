package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	ath "github.com/tuingking/coinbit/aboveThreshold"
	"github.com/tuingking/coinbit/balance"
	"golang.org/x/sync/errgroup"
)

var brokers = []string{"localhost:9092"}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	grp, ctx := errgroup.WithContext(ctx)

	// Create topics if they do not already exist
	balance.PrepareTopics(brokers)
	ath.PrepareTopics(brokers)

	log.Println("starting balance")
	grp.Go(balance.Run(ctx, brokers))
	log.Println("starting aboveThreshold")
	grp.Go(ath.Run(ctx, brokers))

	// Wait for SIGINT/SIGTERM
	waiter := make(chan os.Signal, 1)
	signal.Notify(waiter, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-waiter:
	case <-ctx.Done():
	}
	cancel()
	if err := grp.Wait(); err != nil {
		log.Println(err)
	}
	log.Println("done")
}
