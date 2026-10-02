"""Offline Phase5B1 reference freeze and bounded identity evidence; no trial scoring."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

from modeling.phase5b1_artifacts import OUTPUT, publish, preservation
from modeling.phase5_frozen_reference import freeze_reference, load_reference, validate_bootstrap, bootstrap_results
from modeling.phase5_history_identity import evidence_payloads, load_reconciliation
from modeling.phase5_current_data import verify_checksums
from modeling.refit_preflight import PreflightError, json_bytes, sha256


def reconcile(destination):
    if destination.exists(): raise PreflightError('Never overwrite reconciliation evidence')
    first = evidence_payloads()
    second = evidence_payloads()
    if first!=second: raise PreflightError('Independent reconciliation rebuild differs')
    root = publish(destination,first)
    verify_checksums(destination,expected_checksums_sha256=root)
    # A third independent reconstruction checks the published bytes.
    if any((destination/n).read_bytes()!=b for n,b in evidence_payloads().items()):
        raise PreflightError('Published reconciliation differs from independent rebuild')
    try:
        load_reconciliation(destination,expected_checksums_sha256=root)
    except PreflightError as exc:
        if str(exc)!='Reconciliation remains blocked by unresolved identity evidence': raise
        refusal = str(exc)
    else:
        refusal = None
    return {'checksums_sha256':root,'manifest_sha256':sha256(first['manifest.json']),
            'deterministic_payloads':len(first),'guarded_loader_refusal':refusal,
            'dispositions':json.loads(first['identity_ledger.json'])['disposition_counts']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command',required=True)
    for name in ('reference','reconcile'):
        p = sub.add_parser(name); p.add_argument('--run-name',required=True)
    p = sub.add_parser('verify-reference')
    p.add_argument('--run',type=Path,required=True);p.add_argument('--checksums-sha256',required=True)
    p.add_argument('--manifest-sha256',required=True)
    p = sub.add_parser('verify-reconciliation')
    p.add_argument('--run',type=Path,required=True);p.add_argument('--checksums-sha256',required=True)
    p = sub.add_parser('preserve');p.add_argument('--baseline',type=Path,required=True)
    args = parser.parse_args()
    if hasattr(args,'run_name'):
        if Path(args.run_name).name!=args.run_name or args.run_name in ('','.','..'):
            raise PreflightError('Run name must be one nonempty path component')
        destination = OUTPUT/args.run_name
        result = freeze_reference(destination) if args.command=='reference' else reconcile(destination)
        result['path'] = str(destination)
    elif args.command=='verify-reference':
        bundle = load_reference(args.run,expected_checksums_sha256=args.checksums_sha256,expected_manifest_sha256=args.manifest_sha256)
        frames,_,_ = validate_bootstrap()
        actual = bootstrap_results(bundle.base,bundle.calibrator,bundle.preprocessing,frames)
        if actual!=json.loads((args.run/'bootstrap_parity.json').read_bytes()):
            raise PreflightError('Persisted bootstrap parity failed')
        result = {'status':'VERIFIED','save_load_parity':'EXACT','no_fitting':True}
    elif args.command=='verify-reconciliation':
        verify_checksums(args.run,expected_checksums_sha256=args.checksums_sha256)
        rebuilt = evidence_payloads()
        if any((args.run/n).read_bytes()!=b for n,b in rebuilt.items()):
            raise PreflightError('Reconciliation deterministic rebuild failed')
        try: load_reconciliation(args.run,expected_checksums_sha256=args.checksums_sha256)
        except PreflightError as exc:
            if str(exc)!='Reconciliation remains blocked by unresolved identity evidence':raise
            result = {'integrity':'VERIFIED','fitting_readiness':'BLOCKED','loader_refusal':str(exc)}
        else: result = {'integrity':'VERIFIED','identity_readiness':'READY'}
    else: result = preservation(json.loads(args.baseline.read_bytes()))
    print(json.dumps(result,indent=2,sort_keys=True))


if __name__=='__main__': main()
