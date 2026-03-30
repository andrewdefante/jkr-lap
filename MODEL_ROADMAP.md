## BAPV+ Validation Results — v2.0

Same-season correlations (2025, n=175 pitchers, min 500 pitches + 50 IP):

| Metric | BAPV+ | FG Stuff+ |
|---|---|---|
| vs FIP | -0.648 | -0.619 |
| vs SIERA | -0.741 | -0.737 |
| vs K% | 0.613 | 0.662 |
| vs WAR | 0.495 | 0.444 |
| BAPV+ vs Stuff+ | 0.650 | — |

Cross-season predictive validity (2024 → 2025, n=170 pitchers):

| Predictor | vs FIP '25 | vs SIERA '25 | vs K% '25 | vs WAR '25 |
|---|---|---|---|---|
| BAPV+ '24 | -0.352 | -0.560 | 0.491 | 0.155 |
| Stuff+ '24 | -0.368 | -0.508 | 0.454 | 0.097 |
| FIP '24 (baseline) | 0.458 | — | — | — |
| SIERA '24 (baseline) | — | 0.670 | — | — |

Key findings:

- BAPV+ matches or beats Stuff+ on same-season FIP and SIERA correlation
- BAPV+ outperforms Stuff+ on all cross-season predictive metrics, most notably SIERA (+5.2 points)
- Batter adjustment is the key differentiator — whiffs against contact hitters and called strikes against disciplined batters are weighted more heavily
- Neither metric beats prior-season SIERA for predicting future SIERA (0.670) — outcome metrics contain durability/health information that pitch quality metrics cannot capture
- Biggest model disagreements: ground ball pitchers (Webb, Sale) are underrated by BAPV+ due to whiff-heavy weighting; contact management pitchers (Ober, Miller) are overrated

Known limitations for v2.0:

- Whiff-heavy weighting undervalues ground ball / contact management pitchers
- No xwOBA on contact — uses actual outcomes not expected outcomes
- Called strike weight may still be too low for pitchers like Webb who live on weak contact and called strikes
- Cross-season R² is low (as expected for any single-season pitch metric)
