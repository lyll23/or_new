"""Entire configured pipeline with an in-memory synthetic service; no sockets."""
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from urllib.parse import parse_qs, urlsplit

from or_pipeline.collector import DEFAULT_CONFIG, run_collection
from or_pipeline.http_capture import CaptureClient
from or_pipeline.package import build
from or_pipeline.sync import receive_package
from or_pipeline.ingest import ingest


class SyntheticResponse(io.BytesIO):
    def __init__(self, url, value, status=200, mime='application/json'):
        body = value.encode() if isinstance(value, str) else json.dumps(value).encode()
        super().__init__(body)
        self.url, self.status = url, status
        self.headers = {'Content-Type': mime, 'Content-Length': str(len(body))}

    def geturl(self):
        return self.url

    def getcode(self):
        return self.status


class PipelineTests(unittest.TestCase):
    def test_all_configured_requests_archive_receive_ingest_and_repeat(self):
        models = [{'id': 'synthetic/model' + suffix, 'canonical_slug': 'synthetic/model-v1',
                   'name': 'Synthetic only', 'future_field': {'retained': True}}
                  for suffix in ('', ':free')]

        def transport(url, headers, timeout):
            path, query = urlsplit(url).path, parse_qs(urlsplit(url).query)
            if path == '/api/v1/models':
                return SyntheticResponse(url, {'data': models})
            if path.endswith('/model-activity'):
                row = {'model_permaslug': query['permaslug'][0], 'variant': query['variant'][0],
                       'date': '2026-09-15T00:00:00Z', 'count': 0,
                       'total_prompt_tokens': 9007199254740995, 'total_completion_tokens': 3,
                       'future_counter': 12}
                return SyntheticResponse(url, {'data': {'analytics': [row], 'cachedAt': 1789516800000}})
            if path.endswith('/stats/endpoint'):
                return SyntheticResponse(url, {'error': 'synthetic unauthorized'}, 401)
            if not path.startswith('/api/'):
                return SyntheticResponse(url, '<!doctype html><html><body>Synthetic page without daily data</body></html>', mime='text/html')
            return SyntheticResponse(url, {'data': []})

        def factory(output, **options):
            options.update(min_interval=0, retries=0, transport=transport)
            return CaptureClient(output, **options)

        config = json.loads(DEFAULT_CONFIG.read_text(encoding='utf-8'))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            batch, packages, local = root/'capture', root/'packages', root/'local'
            code = run_collection(batch, config=config, client_factory=factory)
            self.assertEqual(code, 2)  # The two explicit 401 responses stay failures.
            quality = json.loads((batch/'quality.json').read_text())
            self.assertTrue(quality['all_catalog_models_planned'])
            self.assertEqual(quality['models_with_all_requests_attempted'], len(models))
            manifest = build(batch, packages, part_bytes=4096)
            received, receipt = receive_package(manifest.read_bytes(), packages, local)
            self.assertTrue(receipt.exists())
            dbpath = local/'derived'/'observations.sqlite'
            first = ingest(received, dbpath)
            second = ingest(received, dbpath)
            self.assertGreater(first['counts']['new_candidates'], 0)
            self.assertEqual(second['counts']['new_candidates'], 0)
            db = sqlite3.connect(dbpath)
            try:
                daily = db.execute('SELECT count,total_prompt_tokens,variant_raw_json FROM v_table1_daily').fetchall()
                self.assertEqual(len(daily), 2)
                self.assertEqual({x[0] for x in daily}, {'0'})
                self.assertEqual({x[1] for x in daily}, {'9007199254740995'})
                self.assertEqual({x[2] for x in daily}, {'"standard"', '"free"'})
            finally:
                db.close()


if __name__ == '__main__':
    unittest.main()
