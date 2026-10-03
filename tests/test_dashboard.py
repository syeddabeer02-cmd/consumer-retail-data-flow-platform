import json
from decimal import Decimal
from http.server import ThreadingHTTPServer
from threading import Thread
from urllib.error import HTTPError
from urllib.request import urlopen

import duckdb
import pytest

from retail_flow.dashboard import DATASETS, handler, record, snapshot, summary, table


@pytest.fixture
def ledger(tmp_path):
    output = tmp_path / 'runs' / '2026-10-04' / 'test-run'
    with duckdb.connect() as db:
        definitions = {
            'reconciliation': "SELECT 'F1' record_id, 'V000' vendor_id, 'MATCHED' status, 125.75::DECIMAL(18,2) net_payable, 125.75::DECIMAL(18,2) paid_amount, 0::DECIMAL(18,2) variance",
            'funding': "SELECT 'F1' record_id, 'V000' vendor_id, '2026-10-03' event_date, 125.75::DECIMAL(18,2) net_payable",
            'payouts': "SELECT 'LATE1' payout_id, 'F1' record_id, 'V000' vendor_id, '2026-10-04' event_date, 125.75::DECIMAL(18,2) amount",
            'monthly': "SELECT '2026-10' accounting_month, 'V000' vendor_id, 'MATCHED' status, 125.75::DECIMAL(18,2) expected_amount, 125.75::DECIMAL(18,2) paid_amount, 0::DECIMAL(18,2) variance",
            'exceptions': "SELECT 'F2' record_id, 'V001' vendor_id, 'MISSING_PAYOUT' status",
            'quarantine': "SELECT 'funding' source_type, 'invalid_quantity' quality_errors, '{}' raw_record",
        }
        for name, sql in definitions.items():
            directory = output / DATASETS[name]
            directory.mkdir(parents=True)
            db.execute(f"COPY ({sql}) TO '{directory / 'part.parquet'}' (FORMAT PARQUET)")
    manifest = dict(run_id='test-run', batch_date='2026-10-04', completed_at='2026-10-04T02:00:00Z',
                    elapsed_seconds=70, policy_version='synthetic-v1', source_partitions=['2026-10-03', '2026-10-04'],
                    quality={}, quality_gates={'checks': 'PASSED', 'exception_ratio': 0, 'policy': {'max_exception_ratio': 0.5}}, output_counts={'gold_funding': 1},
                    reconciliation={'MATCHED': 1}, inputs={}, output_format='parquet', output_root=str(output))
    published = tmp_path / 'published'
    published.mkdir()
    (published / 'CURRENT_LEDGER.json').write_text(json.dumps(manifest))
    return tmp_path, output, manifest


def test_summary_and_late_payment(ledger):
    root, output, manifest = ledger
    assert snapshot(root)[0] == manifest
    result = summary(manifest, output)
    assert result['totals']['expected'] == Decimal('125.75')
    assert result['statuses'] == {'MATCHED': 1}
    detail = record(output, 'F1')
    assert detail['funding'][0]['event_date'] == '2026-10-03'
    assert detail['payouts'][0]['event_date'] == '2026-10-04'


def test_filters_and_query_boundaries(ledger):
    _, output, _ = ledger
    assert table(output, 'reconciliation', {'status': 'MATCHED', 'search': 'f1'})['total'] == 1
    assert table(output, 'reconciliation', {'search': "' OR 1=1 --"})['total'] == 0
    assert table(output, 'reconciliation', {'page': '2', 'size': '1'})['rows'] == []
    for dataset, args in [('bad', {}), ('funding', {'size': '101'})]:
        with pytest.raises(ValueError):
            table(output, dataset, args)


def test_reject_outside_storage(ledger):
    root, _, manifest = ledger
    manifest['output_root'] = str(root)
    (root / 'published' / 'CURRENT_LEDGER.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        snapshot(root)


def test_http_snapshot_consistency(ledger):
    root, _, _ = ledger
    server = ThreadingHTTPServer(('127.0.0.1', 0), handler(root))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{server.server_port}'
    try:
        with urlopen(base + '/api/summary') as response:
            assert json.load(response)['totals']['paid'] == '125.75'
        with urlopen(base + '/') as response:
            assert b'Retail' in response.read()
        with pytest.raises(HTTPError) as error:
            urlopen(base + '/api/table?run_id=old')
        assert error.value.code == 409
        (root / 'published' / 'CURRENT_LEDGER.json').unlink()
        with pytest.raises(HTTPError) as error:
            urlopen(base + '/api/summary')
        assert error.value.code == 404
        assert json.load(error.value)['empty'] is True
    finally:
        server.shutdown()
        server.server_close()
