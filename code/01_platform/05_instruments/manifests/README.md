# Instrument Manifests — T0 3-slot ready (streaming-3000)

This directory documents the **N=1 (1024) → N=3 (3000 = 1024+1024+952)** sharding model
without requiring a code rewrite. The live manifest remains a single CSV; the
premium 3000 manifests are derived by contiguous chunking at the 1024-token
cap (`MaxHFTTokensPerConnection`).

## Operational live manifest (N=1, default)

- Host: `Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv` (1,024 rows, Jul 31)
- Container: `/instruments/NSE_CM_EQUITY.csv` (via `x-manifest` anchor, env `INSTRUMENT_MANIFEST_HOST_PATH`)
- Env: `ARROW_HFT_CONNECTIONS=1` (default), `INSTRUMENT_MANIFEST_PATH=/instruments/NSE_CM_EQUITY.csv`

No change to default operation — single-socket 1024 continues to work.

## Premium N=3 (3000) candidate

Two equivalent ways to supply 3000 tokens, both auto-sharded by
`BuildSubscriptionPlan` contiguous chunks (`connectionLimit=1024`):

### 1) Single 3000-row CSV (recommended for validation)

Provide one CSV with 3000 rows and set:

```
ARROW_HFT_CONNECTIONS=3
INSTRUMENT_MANIFEST_PATH=/instruments/NSE_CM_EQUITY.csv   # points to the 3000-row CSV
```

`BuildSubscriptionPlan` will shard it as 1024+1024+952 (Slot `hft-0`, `hft-1`, `hft-2`).
No list env required — this is the simplest path for synthetic 3k bench testing
(`GO_FAKE_BROKER=1 …` in `../04_scripts/01_synthetic`).

### 2) Three per-slot CSVs (production premium)

When premium subscription allows 3 concurrent HFT sessions, split the 3000
manifest into three files sharded contiguously 1024+1024+952 and set:

```
ARROW_HFT_CONNECTIONS=3
ARROW_INSTRUMENT_MANIFESTS=/instruments/host/slot1.csv,/instruments/host/slot2.csv,/instruments/host/slot3.csv
```

Compose mounts the host directory `Arrow_broker/instruments/cash_stocks` at
`/instruments/host` (`x-manifest-dir` anchor, env `INSTRUMENT_MANIFESTS_HOST_DIR`),
so `slot1.csv` etc. resolve without extra binds. Until T1 wires the loader to
consume `ARROW_INSTRUMENT_MANIFESTS` as a list, the **single-CSV sharding path**
(above) should be used for 3k validation — list env is forward-compatible.

## Splitting a 3000 CSV

Use the helper:

```bash
python3 code/01_platform/05_instruments/split_manifest.py \
  --input Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv \
  --out-dir Arrow_broker/instruments/cash_stocks \
  --prefix slot --chunks 1024,1024,952
```

For bench (no premium), generate synthetic tokens:

```bash
ARROW_HFT_CONNECTIONS=3 go run ./go-bridge  # with GO_FAKE_BROKER=1 handles 3k via sharding
```

## Env summary

| Env | Default | N=1 (1024) | N=3 (3000) |
|-----|---------|------------|------------|
| `ARROW_HFT_CONNECTIONS` | `1` | `1` | `3` |
| `ARROW_INSTRUMENT_MANIFESTS` | `` (empty = single) | `` | `/instruments/host/slot1.csv,...` or empty if single 3000 CSV |
| `INSTRUMENT_MANIFEST_PATH` | `/instruments/NSE_CM_EQUITY.csv` | default | override to 3000 CSV if using single-CSV path |
| `INSTRUMENT_MANIFEST_HOST_PATH` | host `.../NSE_CM_EQUITY (1024).csv` | default | override to host `.../NSE_CM_EQUITY.csv` (3000) |
| `INSTRUMENT_MANIFESTS_HOST_DIR` | host `.../cash_stocks` | default | default (contains slot*.csv for list path) |

## Why contiguous 1024 chunks?

- Deterministic: `BuildSubscriptionPlan` sorts tokens ascending, slices `i*1024:(i+1)*1024`.
- No reorder: same token always maps to same slot across restarts (when manifest fixed).
- Fingerprint per-slot + manifest-wide hash (`tokenSetHash`) align Java/Go for cross-check.

## T0 validation

```bash
ARROW_HFT_CONNECTIONS=1 docker compose -f code/01_platform/01_docker/docker-compose.yml config | grep ARROW_HFT
ARROW_HFT_CONNECTIONS=3 ARROW_INSTRUMENT_MANIFESTS=/instruments/host/slot1.csv,/instruments/host/slot2.csv,/instruments/host/slot3.csv \
  docker compose -f code/01_platform/01_docker/docker-compose.yml config | grep -E 'ARROW_HFT|MANIFEST'
```

Both should render; N=3 candidate does not need a broker — loader will shard without extra code.

## Swarm production

Image must bake manifests under `/instruments`. For N=3 bake `slot1/2/3.csv` or the single
3000-row CSV and set the corresponding env in the stack. See `docker-stack.yml` comments.
