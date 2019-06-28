# Superdoge

Maintains the current Dogecoin utxo set, indexed by address.

## Running
- Edit config.yaml. Example:
```
node_uri: "http://localhost:22555/"
node_user: dogecoinrpc
node_password: local321
```
- `cargo run --release`

## Get utxos

`GET /utxos`
* query params
  * address - required
  * amount (shibatoshis)
  * minCount - optional - default 20


## Get balance

`GET /balance`

Get balance of address
* query params
  * address - required
