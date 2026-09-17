import copy
import gzip
import hashlib
import io
import json
from pathlib import Path
import stat
import subprocess
import tempfile
import time
import unittest
from unittest import mock
import urllib.error
import zipfile

from or_pipeline import package as P
from or_pipeline import sync as S


def fixture(base, rid='run-20260917-one', part_bytes=2048, status='complete'):
    batch = Path(base) / ('batch-' + rid)
    batch.mkdir()
    (batch / 'raw').mkdir()
    run = {'run_id': rid, 'started_at_utc': '2026-09-17T02:23:00Z', 'status': status}
    (batch / 'run.json').write_text(json.dumps(run), encoding='utf-8')
    for name in ('catalog.json', 'plan.json', 'quality.json'):
        (batch / name).write_text('{}', encoding='utf-8')
    (batch / 'manifest.jsonl').write_text('{"count":9007199254740999}\n', encoding='utf-8')
    (batch / 'raw' / 'x.gz').write_bytes(gzip.compress(b'raw public response\x00' * 7))
    out = Path(base) / ('package-' + rid)
    mp = P.build(batch, out, part_bytes)
    return batch, out, mp, json.loads(mp.read_bytes())


class FakeCloud:
    def __init__(self, packages):
        self.assets = {}
        self.payload = {}
        self.releases = []
        self.metadata_downloads = 0
        self.part_downloads = 0
        for i, (out, mp, m) in enumerate(packages, 1):
            release = {'tag_name': m['release_tag'], 'id': i, 'draft': False, 'prerelease': False}
            self.releases.append(release)
            assets = []
            for j, p in enumerate(sorted(out.iterdir()), 1):
                if p.name.endswith('.building'):
                    continue
                url = 'https://github.com/lyll23/or_new/releases/download/' + str(i) + '/' + p.name
                data = p.read_bytes()
                a = {'name': p.name, 'size': len(data), 'id': i * 1000 + j, 'state': 'uploaded',
                     'updated_at': '2026-09-17T04:00:00Z', 'digest': 'sha256:' + hashlib.sha256(data).hexdigest(), 'browser_download_url': url}
                assets.append(a); self.payload[url] = data
            self.assets[i] = assets

    def pages(self, url, ttl=0):
        if url.endswith('/releases'):
            return iter(self.releases)
        return iter(self.assets[int(url.split('/')[-2])])

    def bytes(self, url, maximum):
        self.metadata_downloads += 1
        value = self.payload[url]
        assert len(value) <= maximum
        return value

    def download(self, asset, path, expected_sha, expected_bytes, force=False):
        self.part_downloads += 1
        b = self.payload[asset['browser_download_url']]
        if len(b) != expected_bytes or hashlib.sha256(b).hexdigest() != expected_sha:
            raise P.PackageError('bad fake download')
        Path(path).write_bytes(b)


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_roundtrip_split_exact_and_idempotent_archive(self):
        batch, parts, mp, m = fixture(self.base, part_bytes=128)
        self.assertGreater(len(m['parts']), 1)
        root = self.base / 'local'
        dest, receipt = S.receive_package(mp.read_bytes(), parts, root)
        self.assertEqual(dest, root / '原始数据/2026/09/17' / m['run_id'])
        for f in m['files']:
            self.assertEqual((dest / f['path']).read_bytes(), (batch / f['path']).read_bytes())
        before = receipt.read_bytes()
        self.assertEqual(S.receive_package(mp.read_bytes(), parts, root)[0], dest)
        self.assertEqual(receipt.read_bytes(), before)
        self.assertFalse((root / '同步记录/入库回执').exists())

    def test_partial_quality_failure_is_archived(self):
        _, parts, mp, _ = fixture(self.base, status='partial')
        _, receipt = S.receive_package(mp.read_bytes(), parts, self.base / 'local')
        self.assertEqual(json.loads(receipt.read_bytes())['collector_status'], 'partial')

    def test_corrupt_part_never_records_receipt(self):
        _, parts, mp, m = fixture(self.base)
        p = parts / m['parts'][0]['name']; p.write_bytes(p.read_bytes()[:-1] + b'!')
        root = self.base / 'local'
        with self.assertRaises(P.PackageError):
            S.receive_package(mp.read_bytes(), parts, root)
        self.assertFalse((root / '同步记录/下载回执').exists())

    def test_missing_part_never_records_receipt(self):
        _, parts, mp, m = fixture(self.base, part_bytes=128)
        (parts / m['parts'][-1]['name']).unlink()
        with self.assertRaises(FileNotFoundError):
            S.receive_package(mp.read_bytes(), parts, self.base / 'local')

    def test_changed_manifest_same_run_refused(self):
        _, parts, mp, m = fixture(self.base)
        root = self.base / 'local'
        S.receive_package(mp.read_bytes(), parts, root)
        m['collector_status'] = 'altered'
        with self.assertRaises(P.PackageError):
            S.receive_package(P.canonical(m), parts, root)

    def test_recover_rename_before_receipt(self):
        _, parts, mp, m = fixture(self.base)
        root = self.base / 'local'; dest, receipt = S.receive_package(mp.read_bytes(), parts, root)
        receipt.unlink()
        recovered, new = S.receive_package(mp.read_bytes(), parts, root)
        self.assertEqual(recovered, dest)
        self.assertEqual(json.loads(new.read_bytes())['status'], 'verified_archived')

    def test_audit_detects_local_raw_edit(self):
        _, parts, mp, _ = fixture(self.base)
        root = self.base / 'local'; dest, _ = S.receive_package(mp.read_bytes(), parts, root)
        (dest / 'raw/x.gz').write_bytes(b'changed')
        with self.assertRaises(P.PackageError):
            S.receive_package(mp.read_bytes(), parts, root, audit_local=True)

    def test_unsafe_paths_and_case_collisions_rejected(self):
        _, _, _, m = fixture(self.base)
        for name in ('../evil', '/evil', 'C:/evil', 'raw\\evil', 'raw/../evil', 'raw/NUL.txt', 'raw/bad.', 'raw/a\x00b'):
            bad = copy.deepcopy(m); bad['files'][1]['path'] = name
            with self.subTest(name=name), self.assertRaises(P.PackageError):
                P.validate_manifest(bad)
        bad = copy.deepcopy(m); bad['files'].append({**bad['files'][0], 'path': bad['files'][0]['path'].upper()})
        with self.assertRaises(P.PackageError): P.validate_manifest(bad)

    def test_nonregular_zip_entry_rejected(self):
        _, _, _, m = fixture(self.base)
        path = self.base / 'symlink.zip'
        with zipfile.ZipFile(path, 'w') as z:
            for f in m['files']:
                i = zipfile.ZipInfo(f['path']); i.create_system = 3; i.external_attr = (stat.S_IFLNK | 0o777) << 16
                z.writestr(i, b'x' * f['bytes'])
        with self.assertRaises(P.PackageError): S.safe_extract(path, self.base / 'stage', m)

    def test_zip_crc_damage_rejected_after_assembly(self):
        _, parts, _, m = fixture(self.base)
        archive = self.base / 'damaged.zip'
        data = bytearray(b''.join((parts / p['name']).read_bytes() for p in m['parts']))
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            info = z.getinfo('raw/x.gz')
            offset = info.header_offset
        name_len = int.from_bytes(data[offset + 26:offset + 28], 'little')
        extra_len = int.from_bytes(data[offset + 28:offset + 30], 'little')
        data[offset + 30 + name_len + extra_len] ^= 1
        archive.write_bytes(data)
        with self.assertRaises((P.PackageError, zipfile.BadZipFile)):
            S.safe_extract(archive, self.base / 'crc-stage', m)

    def test_duplicate_zip_entries_rejected(self):
        _, _, _, m = fixture(self.base)
        path = self.base / 'dup.zip'
        with zipfile.ZipFile(path, 'w') as z:
            for f in m['files']:
                i = zipfile.ZipInfo(m['files'][0]['path']); i.create_system = 3; i.external_attr = (stat.S_IFREG | 0o644) << 16
                z.writestr(i, b'x' * f['bytes'])
        with self.assertRaises(P.PackageError): S.safe_extract(path, self.base / 'stage', m)

    def test_non_utc_run_is_not_silently_relabelled(self):
        with self.assertRaises(P.PackageError): P.run_metadata({'run_id': 'x', 'started_at_utc': '2026-09-17T02:00:00'})
        with self.assertRaises(P.PackageError): P.run_metadata({'run_id': 'x', 'started_at_utc': '2026-09-17T02:00:00+08:00'})

    def test_package_strict_under_2gib(self):
        batch, _, _, _ = fixture(self.base)
        with self.assertRaises(P.PackageError): P.build(batch, self.base / 'more', 2 * 1024**3)

    def test_ingest_failure_retry_and_code_upgrade(self):
        _, parts, mp, _ = fixture(self.base)
        root = self.base / 'local'; S.receive_package(mp.read_bytes(), parts, root)
        fail = mock.Mock(return_value=subprocess.CompletedProcess([], 2))
        one = S.ingest_received(root, runner=fail, signature='v1')
        self.assertEqual(one[0]['status'], 'import_failed')
        def create_database(*args, **kwargs):
            from or_pipeline.ingest import initialize_database
            initialize_database(root / '数据表增量/observations.sqlite').close()
            return subprocess.CompletedProcess([], 0)
        ok = mock.Mock(side_effect=create_database)
        self.assertEqual(S.ingest_received(root, runner=ok, signature='v1')[0]['status'], 'imported')
        self.assertEqual(S.ingest_received(root, runner=ok, signature='v1'), [])
        self.assertEqual(S.ingest_received(root, runner=ok, signature='v2')[0]['status'], 'imported')
        self.assertEqual(ok.call_count, 2)
        self.assertEqual(len(list((root / '同步记录/入库尝试').glob('*.json'))), 3)

    def test_ingest_deleted_or_replaced_database_reimports(self):
        from or_pipeline.ingest import initialize_database
        _, parts, mp, _ = fixture(self.base)
        root = self.base / 'local'; S.receive_package(mp.read_bytes(), parts, root)
        db = root / '数据表增量/observations.sqlite'
        def create(*args, **kwargs):
            initialize_database(db).close()
            return subprocess.CompletedProcess([], 0)
        run = mock.Mock(side_effect=create)
        first = S.ingest_received(root, runner=run, signature='v1')[0]
        self.assertEqual(S.ingest_received(root, runner=run, signature='v1'), [])
        db.unlink()
        second = S.ingest_received(root, runner=run, signature='v1')[0]
        self.assertNotEqual(first['database_instance_id'], second['database_instance_id'])
        db.unlink(); initialize_database(db).close()
        third = S.ingest_received(root, runner=run, signature='v1')[0]
        self.assertNotEqual(second['database_instance_id'], third['database_instance_id'])
        self.assertEqual(run.call_count, 3)

    def test_ingest_zero_exit_without_database_is_not_receipt(self):
        _, parts, mp, _ = fixture(self.base)
        root = self.base / 'local'; S.receive_package(mp.read_bytes(), parts, root)
        r = S.ingest_received(root, runner=mock.Mock(return_value=subprocess.CompletedProcess([], 0)), signature='v1')
        self.assertEqual(r[0]['status'], 'import_failed')
        self.assertIsNone(r[0]['database_instance_id'])

    def test_all_release_history_and_unchanged_fast_path(self):
        _, a, am, m = fixture(self.base, 'run-one')
        _, b, bm, n = fixture(self.base, 'run-two')
        cloud = FakeCloud([(a, am, m), (b, bm, n)])
        root = self.base / 'local'
        first = S.sync('lyll23/or_new', root, client=cloud, do_ingest=False)
        self.assertEqual(first['status'], 'complete'); self.assertEqual(set(first['received_runs']), {'run-one', 'run-two'})
        downloaded = cloud.metadata_downloads
        with mock.patch.object(S, 'verify_existing', side_effect=AssertionError('must not rescan historical raw')):
            second = S.sync('lyll23/or_new', root, client=cloud, do_ingest=False)
        self.assertEqual(cloud.metadata_downloads, downloaded)
        self.assertEqual(second['received_runs'], [])
        self.assertEqual(set(second['already_received_runs']), {'run-one', 'run-two'})
        cloud.assets[1][-1]['updated_at'] = '2026-09-18T00:00:00Z'
        S.sync('lyll23/or_new', root, client=cloud, do_ingest=False)
        self.assertGreater(cloud.metadata_downloads, downloaded)

    def test_missing_cloud_part_partial_not_success(self):
        _, out, mp, m = fixture(self.base)
        cloud = FakeCloud([(out, mp, m)])
        cloud.assets[1] = [a for a in cloud.assets[1] if a['name'] != m['parts'][0]['name']]
        root = self.base / 'local'
        r = S.sync('lyll23/or_new', root, client=cloud, do_ingest=False)
        self.assertEqual(r['status'], 'partial'); self.assertEqual(r['received_runs'], [])
        self.assertFalse((root / '同步记录/下载回执').exists())

    def test_manifest_checksum_mismatch(self):
        _, out, mp, m = fixture(self.base)
        cloud = FakeCloud([(out, mp, m)])
        a = next(a for a in cloud.assets[1] if a['name'].endswith('.sha256'))
        cloud.payload[a['browser_download_url']] = b'0' * 64
        r = S.sync('lyll23/or_new', self.base / 'local', client=cloud, do_ingest=False)
        self.assertEqual(r['status'], 'partial')

    def test_http_pagination_consumes_nonlatest_pages(self):
        client = S.PublicGitHub(self.base / 'cache')
        client.json = mock.Mock(side_effect=[list(range(100)), list(range(100, 107))])
        self.assertEqual(len(list(client.pages('https://api.github.com/repos/x/y/releases'))), 107)
        self.assertIn('page=2', client.json.call_args_list[-1].args[0])

    def test_rate_limit_keeps_failure_and_retry_at_least_30min(self):
        def opener(req, timeout):
            self.assertNotIn('Authorization', req.headers)
            raise urllib.error.HTTPError(req.full_url, 403, 'limited', {'X-RateLimit-Remaining': '0'}, None)
        client = S.PublicGitHub(self.base / 'cache', opener)
        with self.assertRaises(S.RateLimited) as caught:
            client.json('https://api.github.com/repos/x/y/releases')
        retry = S.dt.datetime.fromisoformat(caught.exception.retry_at.replace('Z', '+00:00')).timestamp()
        self.assertGreaterEqual(retry, time.time() + 1799)

    def test_api_etag_304_reuses_bound_value(self):
        class Response(io.BytesIO):
            headers = {'ETag': '"fixture-v1"'}
        seen = []
        def opener(req, timeout):
            seen.append(req)
            if len(seen) == 1:
                return Response(b'[{"id":1}]')
            self.assertEqual(req.get_header('If-none-match'), '"fixture-v1"')
            raise urllib.error.HTTPError(req.full_url, 304, 'unchanged', {}, None)
        client = S.PublicGitHub(self.base / 'cache', opener)
        url = 'https://api.github.com/repos/x/y/releases'
        self.assertEqual(client.json(url, ttl=0), [{'id': 1}])
        self.assertEqual(client.json(url, ttl=0), [{'id': 1}])

    def test_unknown_asset_metadata_cannot_use_fast_skip(self):
        self.assertIsNone(S.asset_fingerprint({'x': {'id': 1, 'size': 1, 'state': 'uploaded', 'browser_download_url': 'https://github.com/x'}}, ['x']))

    def test_monthly_asset_limit_fails_before_upload(self):
        _, out, mp, m = fixture(self.base)
        existing = [{'name': 'old-' + str(i)} for i in range(999)]
        fake = mock.Mock(side_effect=[json.dumps([[{'tag_name': m['release_tag'], 'id': 1, 'draft': False, 'prerelease': False}]]), json.dumps([existing])])
        with mock.patch.object(P, 'gh', fake), self.assertRaises(P.PackageError):
            P.publish(mp, 'lyll23/or_new')
        self.assertEqual(fake.call_count, 2)

    def test_publish_manifest_last_no_clobber(self):
        _, _, mp, m = fixture(self.base)
        calls = []
        def gh(args):
            calls.append(args)
            if args[0] == 'api' and '/releases?' in args[1]: return json.dumps([[{'tag_name': m['release_tag'], 'id': 1, 'draft': False, 'prerelease': False}]])
            if args[0] == 'api': return '[[]]'
            return ''
        with mock.patch.object(P, 'gh', gh): P.publish(mp, 'lyll23/or_new')
        uploads = [c for c in calls if c[:2] == ['release', 'upload']]
        self.assertEqual(Path(uploads[-1][3]), mp)
        self.assertFalse(any('--clobber' in c for c in calls))


if __name__ == '__main__':
    unittest.main()
