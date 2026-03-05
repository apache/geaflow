# Mathematical Modeling Notes (CASTS)

This note summarizes the similarity-threshold formulation used by the
strategy cache for Tier-2 matching.

## Dynamic Similarity Threshold

We compute a dynamic similarity threshold for a strategy knowledge unit (SKU)
using:

```
delta_sim(v) = 1 - kappa / (sigma_logic(v) * (1 + beta * log(eta(v))))
```

Where:

- `eta(v)` is the SKU confidence score.
- `sigma_logic(v)` is the SKU logic complexity.
- `kappa` controls the overall strictness (higher kappa -> lower threshold).
- `beta` controls frequency sensitivity (higher beta -> stricter for high-eta SKUs).

## Design Properties

- `delta_sim(v)` is in (0, 1) and increases with `eta(v)`.
- Higher confidence implies a stricter similarity requirement.
- Higher logic complexity implies a stricter similarity requirement.
- The term with `log(eta)` requires `eta >= 1`; implementations clamp values to
  keep the log domain valid.

## Implementation Reference

The implementation lives in `casts/utils/helpers.py` and is used by
`casts/core/strategy_cache.py` when selecting Tier-2 candidates.
