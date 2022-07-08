package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
	ath "github.com/tuingking/coinbit/abovethreshold"
	"github.com/tuingking/coinbit/balance"
	"github.com/tuingking/coinbit/deposit"
)

func Run(brokers []string, stream goka.Stream) {
	balanceView, err := goka.NewView(brokers, balance.Table, new(balance.BalanceCodec))
	if err != nil {
		panic(err)
	}
	go balanceView.Run(context.Background())

	aboveThresholdView, err := goka.NewView(brokers, ath.Table, new(ath.AboveThresholdCodec))
	if err != nil {
		panic(err)
	}
	go aboveThresholdView.Run(context.Background())

	emitter, err := goka.NewEmitter(brokers, stream, new(deposit.DepositCodec))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	router := mux.NewRouter()
	router.HandleFunc("/deposit", depositMoney(emitter, stream)).Methods("POST")
	router.HandleFunc("/details/{wallet_id}", getBalance(balanceView, aboveThresholdView, emitter)).Methods("GET")

	log.Printf("Listen port 8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}

func depositMoney(emitter *goka.Emitter, stream goka.Stream) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var m deposit.Deposit

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}

		err = json.Unmarshal(b, &m)
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}

		err = emitter.EmitSync(m.WalletID, &m)
		if err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}

		log.Printf("Deposit %s with amount %v\n", m.WalletID, m.Amount)
		fmt.Fprintf(w, "Deposit %s with amount %v\n", m.WalletID, m.Amount)
	}
}

func getBalance(balanceView *goka.View, aboveThresholdView *goka.View, emitter *goka.Emitter) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		walletID := mux.Vars(r)["wallet_id"]

		if err := emitter.EmitSync(walletID, &deposit.Deposit{
			WalletID: walletID,
			Amount:   0,
		}); err != nil {
			fmt.Fprintf(w, "error: %v", err)
			return
		}

		val, _ := balanceView.Get(walletID)
		if val == nil {
			fmt.Fprintf(w, "%s not found!", walletID)
			return
		}
		b := val.(*balance.Balance)

		val, _ = aboveThresholdView.Get(walletID)
		if val == nil {
			fmt.Fprintf(w, "%s not found!", walletID)
			return
		}
		th := val.(*ath.AboveThreshold)

		type response struct {
			WalletID       string  `json:"wallet_id"`
			Balance        float64 `json:"balance"`
			AboveThreshold bool    `json:"above_threshold"`
		}
		resp, _ := json.Marshal(response{
			WalletID:       walletID,
			Balance:        b.Balance,
			AboveThreshold: th.IsAbove,
		})

		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
	}
}
