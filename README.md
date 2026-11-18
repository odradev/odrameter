# Odrameter

Measures how many Casper transactions use [Odra](https://odra.dev): over the last 24 hours, 7 days and
30 days, on mainnet and testnet. The result is a static page.

A transaction counts when it executes at least one contract written in Odra, or installs one.

## Running

Needs Node 22+ and nothing else.

```sh
node odromierz.mjs                 # scan both networks (30 days), build site/index.html
node odromierz.mjs --window 24h    # 24h, 7d or 30d
node odromierz.mjs --networks testnet --concurrency 4
node odromierz.mjs report          # rebuild the page from the cache, offline
```

Scans are incremental. Everything fetched is kept in `data/<network>/`, so the first 30-day scan takes
about three hours and later ones only fetch new blocks. A scan can be interrupted and resumed.

Environment variables: `ODROMIERZ_MAINNET_RPC` and `ODROMIERZ_TESTNET_RPC` override the node URLs,
`ODROMIERZ_AUTH` sets an `Authorization` header for them.

## How it counts

- Every block in the window is fetched from a node's JSON-RPC.
- Native transfers and auction operations (lanes 0 and 1) run no contract code. They count towards the
  total only.
- For every other transaction, the execution effects list each contract that was touched. A reverted
  transaction has no such effects, so its target is used instead.
- A contract is an Odra contract when its wasm contains the Odra installer's argument names
  (`odra_cfg_*`).

## Hosting

`.github/workflows/scan.yml` runs the scan every hour and deploys `site/` to GitHub Pages. The cache is
kept on the `data` branch as a single commit that each run replaces.
