# BestFightOdds Raw Snapshots

Generated only by the bounded one-page snapshot probe:

```bash
python3 warehouse/probe_bestfightodds_snapshot.py --dry-run --event-id ufc-330-4237
python3 warehouse/probe_bestfightodds_snapshot.py --event-id ufc-330-4237
```

The probe writes:

- `data/odds/raw/bestfightodds/<timestamp>_<target>_<id>_<hash>.html`
- `data/odds/raw/bestfightodds/<timestamp>_<target>_<id>_<hash>.metadata.json`

Reviewed snapshots can be adapted offline with:

```bash
python3 warehouse/adapt_bestfightodds_snapshots.py
```

The adapter writes:

- `data/odds/sources/bestfightodds_fight_odds.csv`
- `data/odds/sources/bestfightodds_unmatched_odds.csv`

The line-history adapter reads stored snapshots plus stored payloads:

```bash
python3 warehouse/adapt_bestfightodds_line_history_payloads.py
```

It writes:

- `data/odds/sources/bestfightodds_line_history_fight_odds.csv`
- `data/odds/sources/bestfightodds_line_history_unmatched_odds.csv`

After review, source rows can be promoted to the canonical odds CSV with:

```bash
python3 warehouse/promote_odds_source_to_canonical.py --source-csv data/odds/sources/bestfightodds_line_history_fight_odds.csv
```

Line-history payload probes are stored under:

- `data/odds/raw/bestfightodds/payloads/`

These payloads are generated only by the bounded payload probe:

```bash
python3 warehouse/probe_bestfightodds_line_history_payload.py --dry-run --url 'https://www.bestfightodds.com/js/bfo.min.js?v=0.4.10'
python3 warehouse/probe_bestfightodds_line_history_payload.py --url 'https://www.bestfightodds.com/api/ggd?m=43741&p=1'
```

Guardrails:

- One explicitly requested URL, event identifier, or fighter identifier per run.
- Hard request cap of 1 page.
- Dry-run mode reports the intended fetch without network access.
- Raw HTML is stored for later review; no parser runs here.
- Nothing is loaded into `data/odds/fight_odds.csv` or the warehouse by this
  probe or adapter.
- BestFightOdds open/close rows without precise observed timestamps stay in the
  unmatched review output as `open_close_label_only`.
- Payload probes are limited to one reviewed payload response per run and are
  raw-evidence capture only.
- Line-history adapter output stays source-specific until a separate canonical
  merge decision is made.
- Promoted `BestFightOdds Mean` rows are market-benchmark rows, not
  named-bookmaker executable prices.
- No scheduled or continuous scraping belongs in this folder.
