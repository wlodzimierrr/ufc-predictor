# Phase 2 Normalization Rules

Exact mapping from Phase 1 CSV columns to warehouse DB columns.
All loaders must follow these rules. No loader should make independent decisions about edge cases.

_Source CSVs live in `data/`. Target tables are defined in `warehouse/sql/`._

---

## Null policy

**Empty string in any CSV field → `NULL` in the DB for all nullable columns.**

No loader should store an empty string `""` in the database. Apply this universally before
inserting any row.

---

## events

| CSV column | DB column | Transformation |
|---|---|---|
| `event_id` | `event_id` | UUID, no change |
| `name` | `event_name` | strip whitespace |
| `date_formatted` | `event_date` | parse ISO date string `"YYYY-MM-DD"` → `datetime.date` |
| `city` | `city` | strip whitespace; NULL if empty |
| `state` | `state` | strip whitespace; NULL if empty |
| `country` | `country` | strip whitespace; NULL if empty |
| `url` | `source_url` | no change |
| `scraped_at` | `scraped_at` | parse ISO timestamp |
| `fights` | _(not loaded)_ | comma-separated fight UUIDs; not needed in events table |
| `date` | _(not loaded)_ | human-readable date; `date_formatted` is used instead |
| _(from events_manifest.csv)_ | `event_status` | join on `event_id`; use `event_status` column; default `"completed"` if absent from manifest |

---

## fights

### Winner and result type

Derived from `fighter_1_outcome` and `fighter_2_outcome`:

| fighter_1_outcome | fighter_2_outcome | result_type | winner_fighter_id |
|---|---|---|---|
| `W` | `L` | `win` | `fighter_1_id` |
| `L` | `W` | `win` | `fighter_2_id` |
| `D` | `D` | `draw` | `NULL` |
| `NC` | `NC` | `nc` | `NULL` |

No other outcome combinations have been observed in the data.

### Finish method

Use `primary_finish_method` for the normalized enum. Store `secondary_finish_method` as-is
(free-text detail; 300+ distinct values, not enumerable).

| primary_finish_method (CSV) | finish_method (DB) |
|---|---|
| `decision` | `decision` |
| `ko/tko` | `ko_tko` |
| `submission` | `submission` |
| `tko - doctor's stoppage` | `doctor_stoppage` |
| `overturned` | `overturned` |
| `could not continue` | `could_not_continue` |
| `dq` | `dq` |
| `other` | `other` |

`finish_detail` (DB) ← `secondary_finish_method` (CSV), stored as free text, NULL if empty.

### Weight class and title fight

Derived from `bout_type`. Extract using this ordered logic:

**Step 1 — detect flags:**
- `is_title_fight = True` if `"Title Bout"` or `"Tournament Title Bout"` appears in `bout_type`
- `is_interim_title = True` if `"Interim"` appears in `bout_type`

**Step 2 — extract weight class** by searching for known substrings in this order (longest match first):

| Substring to match | weight_class value |
|---|---|
| `"Women's Strawweight"` | `women_strawweight` |
| `"Women's Flyweight"` | `women_flyweight` |
| `"Women's Bantamweight"` | `women_bantamweight` |
| `"Women's Featherweight"` | `women_featherweight` |
| `"Light Heavyweight"` | `light_heavyweight` |
| `"Super Heavyweight"` | `super_heavyweight` |
| `"Heavyweight"` | `heavyweight` |
| `"Featherweight"` | `featherweight` |
| `"Lightweight"` | `lightweight` |
| `"Welterweight"` | `welterweight` |
| `"Middleweight"` | `middleweight` |
| `"Bantamweight"` | `bantamweight` |
| `"Flyweight"` | `flyweight` |
| `"Strawweight"` | `strawweight` |
| `"Open Weight"` | `open_weight` |
| `"Catch Weight"` | `catch_weight` |

If no known weight class substring is found (e.g. early UFC tournament bouts like
`"UFC 3 Tournament Title Bout"`) → `weight_class = NULL`. Do not invent a value.
These 20–25 rows are early-era fights where the division was not formalized.

### Finish time

`finish_time_seconds` (DB) = `finish_time_minute * 60 + finish_time_second`

Both source columns are already integers in the CSV.

### Column mapping

| CSV column | DB column | Transformation |
|---|---|---|
| `fight_id` | `fight_id` | UUID, no change |
| `event_id` | `event_id` | UUID, no change |
| `fighter_1_id` | `fighter_1_id` | UUID, no change |
| `fighter_2_id` | `fighter_2_id` | UUID, no change |
| _(derived)_ | `winner_fighter_id` | see winner logic above; NULL for draw/nc |
| _(derived)_ | `result_type` | `win` / `draw` / `nc` |
| `bout_type` | `weight_class` | see weight class extraction above; NULL if no match |
| _(derived)_ | `is_title_fight` | bool; True if "Title Bout" in `bout_type` |
| _(derived)_ | `is_interim_title` | bool; True if "Interim" in `bout_type` |
| `num_rounds` | `scheduled_rounds` | integer, no change |
| _(derived)_ | `finish_method` | enum from `primary_finish_method`; see table above |
| `secondary_finish_method` | `finish_detail` | free text; NULL if empty |
| `finish_round` | `finish_round` | integer; NULL if empty |
| _(derived)_ | `finish_time_seconds` | `finish_time_minute * 60 + finish_time_second` |
| `referee` | `referee` | strip whitespace; NULL if empty |
| `url` | `source_url` | no change |
| `scraped_at` | `scraped_at` | parse ISO timestamp |
| `judge_1`, `judge_2`, `judge_3` | _(not loaded in MVP)_ | omit for now; add if needed in Phase 3 |
| `fighter_1_outcome`, `fighter_2_outcome` | _(not loaded)_ | used only to derive winner/result |
| `primary_finish_method` | _(not loaded)_ | used only to derive `finish_method` |

---

## fighters

`wins`, `losses`, `draws`, `no_contests`, `record`, and `fight_ids` are **not loaded**.
These are career totals at scrape time; per-fight records will be computed in Phase 3.

| CSV column | DB column | Transformation |
|---|---|---|
| `fighter_id` | `fighter_id` | UUID, no change |
| `full_name` | `full_name` | strip whitespace |
| `first_name` | `first_name` | strip; NULL if empty |
| `last_names` | `last_name` | strip; NULL if empty (`last_names` → singular `last_name`) |
| `nickname` | `nickname` | strip; NULL if empty |
| `height_cm` | `height_cm` | parse float; NULL if empty |
| `weight_lbs` | `weight_lbs` | parse float; NULL if empty |
| `reach_cm` | `reach_cm` | parse float; NULL if empty |
| `stance` | `stance` | strip; NULL if empty |
| `dob_formatted` | `dob` | parse ISO date `"YYYY-MM-DD"` → `datetime.date`; NULL if empty |
| `url` | `source_url` | no change |
| `scraped_at` | `scraped_at` | parse ISO timestamp |
| `height_ft`, `height_in`, `reach_in` | _(not loaded)_ | `height_cm` and `reach_cm` are preferred |
| `dob` | _(not loaded)_ | human-readable; `dob_formatted` is used instead |
| `wins`, `losses`, `draws`, `no_contests` | _(not loaded)_ | computed per-fight in Phase 3 |
| `record` | _(not loaded)_ | composite string; redundant with the above |
| `fight_ids` | _(not loaded)_ | derivable from the `fights` table |

---

## fight_stats_aggregate and fight_stats_by_round

Both tables share the same column mapping. `fight_stats_by_round` additionally has `round`.

Control time: `control_time_seconds` (DB) = `control_time_minutes * 60 + control_time_seconds`
(both source columns are already integers).

| CSV column | DB column | Transformation |
|---|---|---|
| `fight_stat_id` | `fight_stat_id` | UUID PK (aggregate table only) |
| `fight_stat_by_round_id` | `fight_stat_by_round_id` | UUID PK (by-round table only) |
| `fight_id` | `fight_id` | UUID FK → fights |
| `fighter_id` | `fighter_id` | UUID FK → fighters |
| `round` | `round` | integer (by-round table only) |
| `total_strikes_landed` | `total_strikes_landed` | integer; 0 if empty |
| `total_strikes_attempted` | `total_strikes_attempted` | integer; 0 if empty |
| `significant_strikes_landed` | `sig_strikes_landed` | integer; 0 if empty |
| `significant_strikes_attempted` | `sig_strikes_attempted` | integer; 0 if empty |
| `significant_strikes_landed_head` | `sig_strikes_head_landed` | integer; 0 if empty |
| `significant_strikes_attempted_head` | `sig_strikes_head_attempted` | integer; 0 if empty |
| `significant_strikes_landed_body` | `sig_strikes_body_landed` | integer; 0 if empty |
| `significant_strikes_attempted_body` | `sig_strikes_body_attempted` | integer; 0 if empty |
| `significant_strikes_landed_leg` | `sig_strikes_leg_landed` | integer; 0 if empty |
| `significant_strikes_attempted_leg` | `sig_strikes_leg_attempted` | integer; 0 if empty |
| `significant_strikes_landed_distance` | `sig_strikes_distance_landed` | integer; 0 if empty |
| `significant_strikes_attempted_distance` | `sig_strikes_distance_attempted` | integer; 0 if empty |
| `significant_strikes_landed_clinch` | `sig_strikes_clinch_landed` | integer; 0 if empty |
| `significant_strikes_attempted_clinch` | `sig_strikes_clinch_attempted` | integer; 0 if empty |
| `significant_strikes_landed_ground` | `sig_strikes_ground_landed` | integer; 0 if empty |
| `significant_strikes_attempted_ground` | `sig_strikes_ground_attempted` | integer; 0 if empty |
| `knockdowns` | `knockdowns` | integer; 0 if empty |
| `takedowns_landed` | `takedowns_landed` | integer; 0 if empty |
| `takedowns_attempted` | `takedowns_attempted` | integer; 0 if empty |
| _(derived)_ | `control_time_seconds` | `control_time_minutes * 60 + control_time_seconds` |
| `submissions_attempted` | `submissions_attempted` | integer; 0 if empty |
| `reversals` | `reversals` | integer; 0 if empty |
| `url` | `source_url` | no change |
| `scraped_at` | `scraped_at` | parse ISO timestamp |
| `control_time_minutes`, `control_time_seconds` | _(not loaded)_ | used only to derive `control_time_seconds` |

---

## Known edge cases

| Edge case | Count | Handling |
|---|---|---|
| Early UFC tournament bouts with no weight class | ~20 | `weight_class = NULL`; `is_title_fight = True` |
| Overturned results (`primary_finish_method = "overturned"`) | 57 | `finish_method = "overturned"`; winner/result loaded as-is from outcome columns (the stated outcome, not the corrected one) |
| No-contest fights (`NC` outcome) | 88 | `result_type = "nc"`, `winner_fighter_id = NULL` |
| Draws | 62 | `result_type = "draw"`, `winner_fighter_id = NULL` |
| Doctor stoppages | 79 | `finish_method = "doctor_stoppage"` |
| `could not continue` | 32 | `finish_method = "could_not_continue"` |
| `secondary_finish_method` with 300+ distinct values | many | stored as free text in `finish_detail`; not enumerated |
