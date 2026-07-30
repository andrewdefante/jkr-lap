FEASIBILITY REPORT: NEW PLAYER STATS TRANSFORM
================================================

SUMMARY
Overall feasibility: MEDIUM
Estimated work to implement: 3-4 days
Blockers: 3 critical, 4 moderate, 3 minor

The core logic (union player-games across bat/pit/fld/runners, then join back
to per-stat detail, then aggregate to season level with StatsCalculator
formulas) is sound. But the draft was written against a `sample_*` schema
that does not exist in this database at all, and it has real bugs (bad
aliases, integer-division truncation, non-existent columns) that would need
fixing regardless of the schema issue. None of this is fatal, but it means
the draft is a design sketch, not a near-final query — treat it as such.

CRITICAL GAPS (must fix before implementation)

1. **None of the `sample_*` tables referenced in the draft exist in this
   database.** `sample_boxscore_batting`, `sample_boxscore_pitching`,
   `sample_boxscore_fielding`, `sample_games`, `sample_runners`,
   `sample_game_lineups`, `sample_box_score_fielding` — zero matches in any
   schema (`f1`, `mlb`, `nascar`, `public`). The real tables live in the
   `mlb` schema without the `sample_` prefix: `mlb.boxscore_batting`,
   `mlb.boxscore_pitching`, `mlb.boxscore_fielding`, `mlb.games`,
   `mlb.runners`, `mlb.game_lineups`. Every table reference in the draft
   needs to be rewritten with the `mlb.` schema prefix and the real names
   before it will even parse.

2. **`game_lineups` is not one-row-per-player — the draft's join won't work
   as written.** The real table stores lineups as two JSONB array columns
   (`home_lineup`, `away_lineup`), one row per game, e.g.
   `{"name": "Nico Hoerner", "mlbam_id": 663538, "position": "2B"}` per
   array element. There is no expanded view or table today. The draft's
   `left join sample_game_lineups sgl on pgg.game_pk = sgl.game_pk and
   pgg.player_id = sgl.player_id` assumes a row-per-player shape that must
   first be built via `jsonb_array_elements()` (see Validation 2 query
   below) — either as a one-off CTE inside this transform or as a
   reusable view. Also note: `player_team_id`, `opp_team_id`, and `side`
   are not stored anywhere, even in the JSONB — they must be derived: a
   player in `home_lineup` gets `player_team_id = home_team_id`,
   `opp_team_id = away_team_id`, `side = 'home'` (and the mirror for
   `away_lineup`), pulled from `mlb.games`.

3. **Lineup coverage is only 39% of 2026 regular-season games** (630 of
   1,615 games with a `game_lineups` row at all). Any column sourced from
   the lineup join (`site_id`/`venue_id` is fine since that comes from
   `games`, but `side`, `player_team_id`, `opp_team_id` specifically) will
   be NULL for the other 61% of player-games. Since the draft's `dailybat`
   CTE inner-joins to `player_game_season_union` but left-joins to
   `game_lineups`, batters will still appear in the output, but with those
   three fields null for most rows today. This is a data-completeness gap
   in the lineup ingestion pipeline itself, not something fixable in this
   transform — flag it to whoever owns lineup ingestion.

MODERATE GAPS (need workarounds)

1. **`reached_on_error` does not exist anywhere in the schema** — not in
   `boxscore_batting`, not in `at_bats`, `raw_events`, or any other MLB
   table (checked column names across the whole `mlb` schema). PA will be
   computed as `AB+BB+IBB+HBP+SAC+SF` without the ROE term, understating
   true plate appearances by roughly 1 per ~50-70 PA (league ROE rate).
   Workaround: either derive it from `at_bats`/`raw_events` play-by-play
   (event type ilike '%error%'), or accept the undercount and document it
   as a known limitation, matching the draft's own inline comment.

2. **`innings_pitched` in `boxscore_pitching` is stored in baseball
   notation (e.g. `2.1` = 2⅓ innings), not decimal.** The draft correctly
   avoids this column and instead derives IP from `outs` — this is the
   right call, since none of the StatsCalculator formulas (ERA, WHIP, K9,
   etc.) are baseball-notation-aware; they need a true decimal
   (2.333, not 2.1). Confirmed live: for a pitcher with `outs=7`, the
   stored `innings_pitched` is `2.1` while `outs/3` (correct) is `2.333`.
   **However, the draft's exact expression `round(spp.outs / 3, 2)` has a
   Postgres integer-division bug** — `outs` and `3` are both integers, so
   `outs / 3` truncates to an integer *before* `round()` ever sees it.
   Verified live: `outs=7` → `round(7/3, 2)` returns `2.00`, not `2.33`.
   Fix: cast explicitly — `round(outs::numeric / 3, 2)`.

3. **`position` is not tracked per game-appearance in
   `boxscore_fielding`** (no `position` column there — confirmed 14 columns,
   none named `position`). It only exists as `primary_position` on
   `mlb.players` (a player's current/primary position, not what they played
   in a specific game) and inside the `game_lineups` JSONB
   (`player->>'position'`, only for the 39% of games with lineup data).
   If per-game position is required for `dailydef`, it will only be
   partially derivable via the lineup JSONB, with the same 61% coverage
   gap as Gap 3 above.

4. **`runners` has no `team_id` column at all** (confirmed — 19 columns,
   none named `team_id`). The draft's `rn_game` CTE already anticipates
   this by commenting out `team_id`, which is correct, but it means any
   downstream code that expects `player_game_season_union` rows sourced
   from `rn_game` to carry a team affiliation will need to backfill it via
   a join to `boxscore_batting`/`boxscore_pitching`/`boxscore_fielding` or
   `game_lineups`, since a pinch-runner-only appearance has no other source
   of team_id in this schema.

MINOR GAPS (easy fixes)

1. **Undefined table alias `ppg` used throughout instead of `pgg`.** The
   CTE is aliased `pgg` (from `player_game_season_union pgg`), but lines
   reference `ppg.team_id` / `ppg.player_id` / `ppg.game_pk` in `dailybat`
   (join condition and fielding left-join), `dailypit` (join condition),
   and `dailydef` (join condition). This isn't just a typo to clean up —
   as written it would fail to parse/execute in Postgres (`ppg` is not a
   table in scope). Every occurrence needs to become `pgg`.

2. **Inconsistent table naming for the fielding table within the same
   draft.** `dailybat` joins `sample_boxscore_fielding` (no underscore
   between "box" and "score"), while `dailypit` and `dailydef` join
   `sample_box_score_fielding` (with the underscore). Neither exists in
   the real schema (Critical Gap 1), but even after remapping to
   `mlb.boxscore_fielding`, all three CTEs should reference the same name
   consistently.

3. **`spp.wild_pitches` is selected twice in `dailypit`** (duplicate
   column in the CTE's select list) — harmless in most engines that
   dedupe visually but will produce a duplicate output column and should
   just be de-duplicated to one reference.

FORMULA COMPATIBILITY

All PHP StatsCalculator formulas checked against `mlb.boxscore_batting` /
`mlb.boxscore_pitching`:

| Stat | Required Columns | Available? |
|------|-------------------|------------|
| AVG | hits, at_bats | ✅ both exist |
| OBP | hits, walks, hit_by_pitch, sac_flies, at_bats | ✅ all exist |
| SLG | total_bases, at_bats | ✅ both exist (total_bases is even pre-stored) |
| OPS | OBP + SLG | ✅ derived, no new columns needed |
| TB | hits, doubles, triples, home_runs | ✅ all exist (also pre-stored as `total_bases`) |
| PA | at_bats, walks, hit_by_pitch, sac_flies, sac_bunts, reached_on_error | ⚠️ everything exists except `reached_on_error` (Moderate Gap 1) |
| ERA | earned_runs, innings_pitched | ⚠️ `earned_runs` exists; IP must be derived from `outs` with a numeric cast (Moderate Gap 2) |
| WHIP | walks, hits, innings_pitched | ⚠️ same IP-derivation caveat as ERA |
| K9 | strikeouts, innings_pitched | ⚠️ same IP-derivation caveat |
| BB9 | walks, innings_pitched | ⚠️ same IP-derivation caveat |
| HR9 | home_runs, innings_pitched | ⚠️ same IP-derivation caveat |
| K% | strikeouts, batters_faced | ✅ both exist |
| BB% | walks, batters_faced | ✅ both exist |
| IP | outs (÷3) | ⚠️ column exists; draft's cast is buggy (Moderate Gap 2) |

Overall: no formula is blocked by a genuinely missing input column except
PA's `reached_on_error` term. Every other ⚠️ is the same single root cause
(IP must come from `outs::numeric / 3`, not the stored `innings_pitched`,
and not integer division).

JOIN LOGIC

game_lineups coverage: 39.0% of games (630 / 1,615 in 2026 regular season)
have any `game_lineups` row at all.
Substitute/non-starter players not in the lineup JSONB: 148 of 682 total
batters in 2026 (534 matched to a starting lineup slot, 148 did not) — this
count is on top of the 61% of games with no lineup row at all, so total
"players with no lineup-derived team/side info" is larger than 148 across
the full season.

RECOMMENDATIONS

1. Rewrite every table reference in the draft to point at the real `mlb.`
   schema tables (Critical Gap 1) before anything else — nothing else can
   be validated end-to-end until the query actually runs.
2. Build a small reusable view/CTE that expands `game_lineups` JSONB into
   one row per player with `player_team_id`, `opp_team_id`, `side` derived
   from `home_team_id`/`away_team_id` on `mlb.games` (Critical Gap 2) —
   this will be needed by `dailybat`, `dailypit`, and `dailydef` alike, so
   build it once.
3. Standardize IP as `round(outs::numeric / 3, 3)` everywhere a formula
   needs it, and never use the stored `innings_pitched` column as a formula
   input (Moderate Gap 2) — reserve that column for display purposes only.
4. Decide now whether to accept the PA undercount from missing
   `reached_on_error` (Moderate Gap 1) or invest in deriving it from
   `raw_events`/`at_bats` — this affects season PA totals for every batter,
   so it's worth a product decision rather than a silent gap.
5. Fix the `ppg`→`pgg` alias bug and the duplicate `wild_pitches` column
   (Minor Gaps 1, 3) as part of the same rewrite pass — these will hard-fail
   the query otherwise.
6. Loop in whoever owns lineup ingestion about the 39% coverage number —
   that's a data pipeline gap, not something this transform can work around.

SUGGESTED IMPLEMENTATION ORDER

Phase 1: Rewrite table/column references to match real `mlb` schema; fix
alias bugs, duplicate columns, and the IP integer-division bug. Get the
`bat_game`/`pit_game`/`fld_game`/`rn_game`/`player_game_season_union` CTEs
and the three daily-detail CTEs (`dailybat`, `dailypit`, `dailydef`)
running end-to-end without errors.

Phase 2: Build the `game_lineups` JSONB-expansion CTE/view (player_team_id,
opp_team_id, side) and wire it into all three daily-detail CTEs in place of
the nonexistent `sample_game_lineups` join. Decide and document the PA /
`reached_on_error` handling.

Phase 3: Implement the season-level aggregation CTEs (`daily_bat_ytd`,
`daily_pit_ytd`, `daily_def_ytd`) that currently only have comments as
placeholders, applying the StatsCalculator-equivalent formulas (AVG, OBP,
SLG, OPS, ERA, WHIP, K9, BB9, HR9, K%, BB%) as SQL expressions, then union
them into the final `players_stats` table.
