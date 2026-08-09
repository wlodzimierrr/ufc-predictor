# UFC Fight Prediction Data Platform

End-to-end data engineering and machine learning project for predicting UFC fight outcomes.

This repository contains the backend of the system: data collection, warehouse loading,
feature engineering, model training, calibrated prediction generation, and post-event
accuracy tracking. The public dashboard is maintained separately in
[wlodzimierrr/ufc-dashboard](https://github.com/wlodzimierrr/ufc-dashboard).

Live next-event predictions and current model accuracy stats are available at:

https://ufc.wlodzimierrr.pl

## Project Summary

The goal of this project is to turn raw UFCStats data into a repeatable prediction
pipeline that can score upcoming UFC bouts and then measure how those predictions perform
after the event finishes.

What this project demonstrates:

- Building a repeatable data pipeline from scraped sports data into a Postgres warehouse.
- Designing historical, leakage-safe features for fight prediction.
- Training and comparing multiple machine learning models using chronological train,
  validation, and test splits.
- Calibrating model probabilities so the output is useful as a probability, not only as
  a winner pick.
- Publishing upcoming predictions to downstream dashboard/data consumers.
- Tracking post-event accuracy to monitor model quality over time.

## What I Built

### Data Pipeline

- Scrapes UFC event, fighter, fight, and round-level statistics from UFCStats.
- Stores source snapshots as CSV files under `data/`.
- Loads cleaned and transformed data into a Postgres warehouse.
- Includes validation checks for warehouse integrity and consistency.
- Handles incremental refreshes for newly completed and upcoming events.

### Feature Engineering

The feature pipeline builds pre-fight snapshots so the model only sees information that
would have been available before the bout happened.

Feature groups include:

- Fighter career record and win rate.
- Recent form over the last fights.
- Striking and grappling metrics.
- Time-decayed performance metrics.
- Elo-style fighter strength.
- Opponent-adjusted history.
- Physical attributes such as age, height, and reach.
- Debut-fighter priors for bouts with limited historical information.

### Modeling

The modeling layer trains and evaluates several approaches, including:

- Logistic regression baseline.
- LightGBM models.
- XGBoost model.
- Stacked/blended ensemble experiments.
- Calibration with Platt scaling.
- Uncertainty and confidence-tier analysis.

The active production model is selected through `models/production_model.json`. More
detailed model documentation is available in `docs/model_card.md`.

### Prediction And Review Loop

The system can score upcoming fights, save predictions, and later compare those predictions
against actual results.

Outputs include:

- Calibrated win probability.
- Predicted winner.
- Confidence tier.
- Uncertainty flag.
- Event-level and fight-level post-event accuracy reports.
- A real pre-event prediction log built only from predictions saved before event day.
- Historical backtests of the current production model.

## Repository Structure

| Path | Purpose |
|---|---|
| `scraper/` | UFCStats scraping project and raw scraper outputs. |
| `data/` | CSV data snapshots, manifests, and generated reports. |
| `warehouse/` | Postgres migrations, loaders, transforms, and validation checks. |
| `features/` | Historical and upcoming fight feature engineering. |
| `modeling/` | Training, calibration, scoring, uncertainty analysis, and backtesting. |
| `models/` | Model artifacts, production pointer, predictions, and reports. |
| `notebooks/` | Exploration, model review, upcoming prediction, and saved prediction review notebooks. |
| `docs/` | Model card, project notes, and implementation tickets. |

## Dashboard

This repository is the data and ML backend. The dashboard frontend is in:

[wlodzimierrr/ufc-dashboard](https://github.com/wlodzimierrr/ufc-dashboard)

The dashboard displays upcoming fight predictions and current model accuracy statistics:

https://ufc.wlodzimierrr.pl

The dashboard should read from the Postgres reporting views rather than local CSV exports:

- `current_event_predictions` for upcoming/current fight cards.
- `pre_event_prediction_fights` for fight-level post-event review.
- `pre_event_prediction_events` for event-level model accuracy.
- `fighter_career_summary` for fighter profile and comparison panels.

## Tech Stack

- Python
- pandas, NumPy, scikit-learn
- XGBoost, LightGBM
- SHAP for model explanation work
- PostgreSQL
- psycopg2
- pytest
- Jupyter notebooks
- Power BI dashboard assets

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

Build the log of events that were actually predicted before they happened:

```bash
make pre_event_log
```

This writes:

- `data/reports/pre_event_prediction_events.csv`
- `data/reports/pre_event_prediction_fights.csv`

These files are intentionally separate from `models/backtests/`, which are retroactive
historical model scores rather than predictions made before the event.

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

## Refresh After An Event

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

## Limitations

- The model is intended for pre-fight analytical review, not live in-fight prediction.
- It does not include injuries, training camp changes, weight cut issues, short-notice
  replacement context, or betting market odds.
- Fight prediction is noisy by nature, so the project emphasizes calibrated probabilities,
  uncertainty bands, and post-event accuracy tracking rather than only winner picks.
