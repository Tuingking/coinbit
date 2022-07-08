# Coinbit

## Getting Started

1. start kafka
2. start service: `go run cmd/service/main.go`
3. start processor: `go run cmd/processor/main.go`

## Endpoint

1. [GET] /details/{wallet_id}
   ```
   curl -X GET http://localhost:8080/details/foo
   ```

2. [POST] /deposit
   ```
    curl -X POST http://localhost:8080/deposit --data '{"wallet_id": "foo", "amount": 2000.0}'
    ```

