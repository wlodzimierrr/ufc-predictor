# Superseded prospective directory

This directory has no accepted manifest or frozen CSVs. The recovered 146-row
population includes 38 predictions scored after event day and cannot accurately
be described as wholly prospective.

The corrected, accepted schema-version-2 datasets are:

- [Historical comparison population](../historical_2026_apr_aug/README.md):
  146 mixed pre-event/catch-up records, with eight unresolved identity mappings
  explicitly uncertified for clean model evaluation.
- [Pre-event subset](../pre_event_2026_apr_aug/README.md): the exact 108 records
  with original pre-event provenance and a UTC scoring date before event date;
  primary population for later original-forecast comparison.

Threshold high confidence is 44/30 in the historical population. Strongest 57,
ranked by calibrated probability margin, is a separate 57/39 metric. Probability
thresholds and original records were preserved.

See `docs/implementation-reports/phase1b-holdout-freeze-report.md`. The original
Phase 1 report remains an audit record. The validator defaults to the historical
population; training exclusions default to all 146 originals plus verified and
precautionary alternate IDs, and read no outcomes or joined historical evidence.
