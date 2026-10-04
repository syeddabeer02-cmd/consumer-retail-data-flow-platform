"""Verify replay and failure protection inside the existing scheduler container."""
import argparse
import json
from pathlib import Path

from retail_flow.pipeline import run, spark_session
from retail_flow.quality import QualityFailure, QualityPolicy


def read_manifest(root):
    return json.loads((root / 'published' / 'CURRENT_LEDGER.json').read_text())


def financial_totals(manifest):
    spark = spark_session(master='local[2]')
    try:
        frame = spark.read.parquet(str(Path(manifest['output_root']) / 'gold_reconciliation'))
        return frame.selectExpr('sum(net_payable) expected', 'sum(paid_amount) paid',
                                'sum(variance) variance').first().asDict()
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default='/opt/airflow/data')
    args = parser.parse_args()
    root = Path(args.root)
    before = read_manifest(root)
    if before['output_format'] != 'parquet':
        raise ValueError('Scenario verification requires the local Parquet workflow')
    totals = financial_totals(before)
    after = run(str(root), before['batch_date'], master='local[2]')
    for field in ['inputs', 'quality', 'output_counts', 'reconciliation']:
        if before[field] != after[field]:
            raise RuntimeError(f'Replay changed {field}')
    if financial_totals(after) != totals:
        raise RuntimeError('Replay changed financial totals')
    pointer = (root / 'published' / 'CURRENT_LEDGER.json').read_bytes()
    # Force a gate failure without altering any source files. A clean ledger
    # uses an impossible minimum funding count instead of an exception limit.
    policy = QualityPolicy(max_exception_ratio=0) if after['output_counts']['exceptions'] else QualityPolicy(
        min_funding_records=after['quality']['funding']['valid'] + 1)
    try:
        run(str(root), after['batch_date'], master='local[2]', quality_policy=policy)
    except QualityFailure:
        pass
    else:
        raise RuntimeError('Expected quality rejection did not occur')
    if (root / 'published' / 'CURRENT_LEDGER.json').read_bytes() != pointer:
        raise RuntimeError('Failed run replaced successful publication')
    print(json.dumps({'replay': 'PASSED', 'financial_totals': 'UNCHANGED',
                      'failure_protection': 'PASSED', 'published_run_id': after['run_id']}, indent=2))


if __name__ == '__main__':
    main()
