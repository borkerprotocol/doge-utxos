# DOGE-utxos

Maintains the current Dogecoin utxo set, indexed by address.

## Get utxos

`GET /utxos`
* query params
  * address - required
  * page - optional - default 1
  * pageSize - optional - default 100


## Get utxos for target amount

`GET /utxos/amount`

Grabs utxos in batches from the DB until the the sum of their values is >= amount. Otherwise throws `insufficient funds` error. 
* query params
  * address - required
  * amount (in sibatoshis) - required
  * batchSize - optional - default 10
