"""Winner abstention metadata must not alter market value or staking policy."""

from betting.recommend import apply_bet_pass_policy, build_recommendation_inputs
from betting.tests.test_recommend import AS_OF, _prediction, _valid_odds
from modeling.decisions import decide_prediction


def test_no_pick_metadata_preserves_value_and_staking():
    original = _prediction(calibrated_prob_f1="0.54", confidence_tier="toss-up", is_uncertain=True)
    enriched = {**original, **decide_prediction(.54, "Fighter A", "Fighter B")}
    assert enriched["pick_label"] is None
    baseline = build_recommendation_inputs([original], _valid_odds(), as_of=AS_OF)
    result = build_recommendation_inputs([enriched], _valid_odds(), as_of=AS_OF)
    assert result == baseline
    assert apply_bet_pass_policy(result) == apply_bet_pass_policy(baseline)
