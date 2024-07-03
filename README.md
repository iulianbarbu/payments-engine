# Payments engine

A payments engine which holds account and transaction ledgers and execute operations like deposits, withdrawals, disputes for previous deposits, resolving and chargebacks for disputes.

# Overall system characteristics

## Efficiency

Transactions are read/processed from a CSV input as a stream through an asynchronous multi-threaded runtime, by using
`tokio`, `csv-async` and `serde`. 

## Safety and robustness

All operations over numbers with decimals are done by using `BigDecimal` struct from `bigdecimal` crate. We shouldn't be
concerned with overflow because of this, but underflow can be possible, so we defend against it by emitting an error and
gracefully handling it so that we don't error out and stop the stream processing because of an invalid transaction.

## Maintainability

The code base relies on a few abstractions:
* AccountsDal - a data access layer which provides an interface over all clients' accounts by not being concerned with
  the underlying storage solution.
* TxsDal - a data access layer similar to the `AccountsDal` but for transactions storage.
* The `Engine::handle_txs` method which processes TXs by consuming them from a stream received as a parameter, that an
  implementation for `AsyncRead + Send + Unpin`. This is the basis for consuming TXs from both a `tokio::io::File` and `tokio::io::TcpStream`, so the business logic should be usable inside an asynchronous multi-threaded web server.

## Correctness

The payments engine main logic is tested through unit tests for every transaction and the majority of corner cases worth
testing.