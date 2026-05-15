# UFC Data

Data, feature engineering, modeling, and prediction pipelines for UFC fight analytics.

This repository owns the backend data work for the UFC prediction system: scraping outputs,
warehouse loading, feature generation, model training, upcoming fight scoring, and post-event
accuracy review.

The dashboard UI is maintained separately in the
[wlodzimierrr/ufc-dashboard](https://github.com/wlodzimierrr/ufc-dashboard)
repository. Next event predictions and current model accuracy stats are published at:

https://ufc.wlodzimierrr.pl

## What is in this repo

| Path | Purpose |
|---|---|
| `scraper/` | UFCStats scraping project and raw scraper outputs. |
| `data/` | CSV data snapshots and generated reports. |
| `warehouse/` | Postgres schema migrations, loaders, transforms, and validation checks. |
| `features/` | Fight-level and fighter-level feature engineering for historical and upcoming bouts. |
| `modeling/` | Model training, calibration, scoring, uncertainty analysis, and backtesting. |
| `models/` | Trained model artifacts, production model pointer, predictions, and reports. |
| `notebooks/` | Exploratory analysis, model review, upcoming prediction, and saved prediction review notebooks. |
| `docs/` | Model card, project tickets, and phase notes. |

## Model Overview

The production model is selected through `models/production_model.json`.

Current model documentation lives in `docs/model_card.md`. At the time of that model card,
the production model is a calibrated XGBoost classifier trained on chronological UFC fight
history and evaluated on held-out recent events.

Primary outputs:

- Calibrated probability that `fighter_1` wins.
- Predicted winner probability.
- Confidence tier.
- Uncertainty flag.
- Post-event accuracy and calibration reports.

## Setup

Create a Python environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a local environment file:

```bash
cp .env.example .env
```

Then fill in the Postgres settings:

```bash
POSTGRES_HOST=
POSTGRES_PORT=5432
POSTGRES_DB=
POSTGRES_USER=
POSTGRES_PASSWORD=
```

## Common Workflows

Run database migrations:

```bash
make migrate
```

Load all historical warehouse tables:

```bash
make load_all
```

Validate warehouse integrity and consistency:

```bash
make warehouse_check
```

Build historical features:

```bash
make build_features
```

Build and score upcoming fights:

```bash
make predict_pipeline
```

Score predictions through the CLI:

```bash
python predict.py
python predict.py --next
python predict.py --event "UFC 315"
python predict.py --format json
```

Review a completed event:

```bash
make review_event EVENT="UFC 315"
```

Backtest completed historical events with the production model:

```bash
make backtest_past
```

## Training

Train the main model family and compare candidates:

```bash
make train_all_v2
```

Individual training targets are also available:

```bash
make train_logreg
make train_lgbm
make train_lgbm_v2
make train_xgb
make train_ensemble
make compare_models
```

Generated artifacts are written under `models/`, including:

- `models/production_model.json`
- `models/*/<timestamp>/model.joblib`
- `models/*/<timestamp>/metadata.json`
- `models/*/<timestamp>/metrics.json`
- `models/comparison_report.txt`
- `models/predictions/<event_date>/predictions.csv`
- `models/backtests/`

## Refresh After an Event

After an event completes, refresh scraped data, reload the warehouse, rebuild upcoming
features, and regenerate predictions:

```bash
make post_event
```

## Tests

Run the focused test suites:

```bash
python -m pytest features/tests warehouse/tests modeling/tests tests
```

Run the integration pipeline test:

```bash
make test_integration
```

## Notes

- The model is intended for pre-fight analytical review, not live in-fight prediction.
- Predictions do not include injuries, late camp changes, weight cut issues, betting markets,
  or other off-platform context.
- Public-facing dashboard work belongs in the separate
  [wlodzimierrr/ufc-dashboard](https://github.com/wlodzimierrr/ufc-dashboard)
  repository.
