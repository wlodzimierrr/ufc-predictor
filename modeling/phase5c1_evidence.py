"""Byte-bound, identity-bound structured evidence. No generated-text inference."""
from datetime import date
import json
from modeling.phase5c1_contract import decoded
from modeling.phase5c1_contract import Blocked, IDENTITY, digest, instant, local_bytes, require, sha

DOMAIN = 'admitted_resolved_ufc_occurrences_v1'


def validate_evidence(root, evidence, cutoff):
    require(evidence.get('version') == 'phase5c1_evidence_package_v1', 'evidence_schema')
    require(isinstance(evidence.get('sources'), dict) and isinstance(evidence.get('assertions'), list), 'evidence_schema')
    bodies = {}
    for sid, s in evidence['sources'].items():
        raw = local_bytes(root, s['body'])
        require(raw and sha(raw) == s['sha256'], 'evidence_hash_mismatch')
        require(s.get('provider') and s.get('url') and s.get('authoritative') is True, 'unsupported_provider_assertion')
        require(s.get('status') == 200 and s.get('access_blocked') is False, 'access_check_not_evidence')
        require(instant(s['requested_at']) <= instant(s['observed_at']) <= instant(cutoff), 'evidence_chronology')
        lower = raw.lower()
        require(not any(x in lower for x in (b'just a moment', b'checking your browser', b'access denied', b'captcha')), 'access_check_not_evidence')
        bodies[sid] = raw
    keys = set()
    for a in evidence['assertions']:
        require(a.get('kind') in {'title', 'experience', 'identity'}, 'unsupported_assertion')
        require(a.get('source_id') in bodies, 'assertion_missing_body')
        sid = a['source_id']
        require(a.get('observed_at') == evidence['sources'][sid]['observed_at'], 'assertion_observation_mismatch')
        require(a.get('extraction', {}).get('method') == 'json_pointer_v1', 'unsupported_extraction_or_review')
        # A preserved authoritative structured export, including a signed review
        # export, must contain the entire explicit claim. "Bout" is never parsed.
        try:
            claim = decoded(bodies[sid])
            pointer = a['extraction']['pointer']
            require(pointer.startswith('/'), 'invalid_json_pointer')
            for part in pointer[1:].split('/'):
                part = part.replace('~1', '/').replace('~0', '~')
                claim = claim[int(part)] if isinstance(claim, list) else claim[part]
        except (ValueError, KeyError, TypeError, IndexError) as exc:
            raise Blocked('unreproducible_assertion') from exc
        require(claim == a['claim'] and digest(claim) == a['extraction']['claim_sha256'], 'unsupported_assertion')
        if evidence['sources'][sid].get('review'):
            review = evidence['sources'][sid]['review']
            require(review.get('reviewer') and review.get('authority') and review.get('signed_assertion_sha256') == digest(claim), 'unsigned_review')
            original = local_bytes(root, review['original_body'])
            require(sha(original) == review['original_sha256'] and original, 'review_original_hash')
            require(review.get('original_provider') and review.get('original_url'), 'review_original_provider_identity')
            require(instant(review['original_observed_at']) <= instant(review['reviewed_at']) <= instant(evidence['sources'][sid]['observed_at']) <= instant(cutoff), 'review_after_cutoff')
            require(not any(x in original.lower() for x in (b'just a moment', b'checking your browser', b'access denied', b'captcha')), 'access_check_not_evidence')
            for span in review['spans']:
                require(original[span['start']:span['end']].decode('utf-8') == span['text'], 'review_span_mismatch')
            require(review['spans'], 'review_missing_spans')
        c = a['claim']
        if a['kind'] in {'title', 'experience'}:
            require(all(c.get(k) for k in IDENTITY), 'evidence_identity_missing')
        if a['kind'] == 'title':
            require(type(c.get('is_title_fight')) is bool, 'unknown_title_status')
            key = ('title', c['fight_id'])
        elif a['kind'] == 'experience':
            require(c.get('domain') == DOMAIN and c.get('status') in {'verified_history', 'verified_debut'}, 'experience_domain_or_status')
            require(c.get('complete') is True and isinstance(c.get('prior_occurrences'), list), 'experience_incomplete')
            date.fromisoformat(c['covered_from'])
            date.fromisoformat(c['covered_before_exclusive'])
            require(c['covered_from'] <= '1993-11-12' and c['covered_from'] < c['covered_before_exclusive'], 'experience_period')
            require(c['fighter_id'] in (c['fighter_1_id'], c['fighter_2_id']), 'experience_identity')
            key = ('experience', c['fight_id'], c['fighter_id'])
        else:
            require(c.get('relationship') in {'same_occurrence', 'distinct_occurrences'}, 'unsupported_identity_relationship')
            key = ('identity', tuple(sorted(c['fight_ids'])))
        require(key not in keys, 'conflicting_or_duplicate_assertions')
        keys.add(key)
    return evidence


def essential_reasons(target, history, profiles, evidence, cutoff):
    reasons = []
    claims = [a for a in evidence['assertions'] if a['kind'] in {'title', 'experience'} and a['claim']['fight_id'] == target['fight_id']]
    for a in claims:
        require(all(a['claim'][k] == target[k] for k in IDENTITY), 'mismatched_evidence_identity')
        require(a['claim']['event_date'] == target['event_date'], 'mismatched_evidence_event_date')
    titles = [a['claim'] for a in claims if a['kind'] == 'title']
    if len(titles) != 1:
        reasons.append('unknown_title_status')
    boundary = min(target['event_date'], instant(cutoff).date().isoformat())
    for side in (1, 2):
        pid = target[f'fighter_{side}_id']
        if pid not in profiles:
            reasons.append(f'missing_fighter_{side}_profile')
        experiences = [a['claim'] for a in claims if a['kind'] == 'experience' and a['claim']['fighter_id'] == pid]
        if len(experiences) != 1:
            reasons.append(f'unverified_fighter_{side}_experience')
            continue
        e = experiences[0]
        require(e['covered_before_exclusive'] == boundary, 'experience_coverage_cutoff_mismatch')
        actual = sorted([{'fight_id': f['fight_id'], 'event_id': f['event_id'], 'event_date': f['event_date'],
                          'fighter_1_id': f['fighter_1_id'], 'fighter_2_id': f['fighter_2_id'], 'result_type': f['result_type']}
                         for f in history if f['event_date'] < boundary and pid in (f['fighter_1_id'], f['fighter_2_id'])], key=lambda f: f['fight_id'])
        require(sorted(e['prior_occurrences'], key=lambda f: f['fight_id']) == actual, 'experience_history_mismatch')
        require(e['status'] != 'verified_debut' or not actual, 'missing_history_is_not_debut')
    if not target.get('weight_class'):
        reasons.append('unknown_weight_class')
    return reasons, titles[0]['is_title_fight'] if len(titles) == 1 else None
