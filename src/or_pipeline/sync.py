"""Token-free, paginated public Release backfill, verified archive, then local ingest."""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile

from .package import (PackageError, REQUIRED, atomic_json, canonical, file_stat,
                      run_metadata, safe_relative, sha256, utc_now, validate_manifest)

RELEASE = re.compile(r'research-data-\d{4}-\d{2}\Z')
MAX_MANIFEST_BYTES = 64 * 1024 * 1024


class RateLimited(RuntimeError):
    def __init__(self, retry_at):
        self.retry_at = retry_at
        super().__init__('Public API rate limited; retry at ' + retry_at)


class AlreadyRunning(RuntimeError):
    pass


def utc_from_epoch(value):
    return dt.datetime.fromtimestamp(value, dt.timezone.utc).isoformat().replace('+00:00', 'Z')


@contextlib.contextmanager
def sync_lock(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    f = path.open('a+b')
    f.seek(0, 2)
    if f.tell() == 0:
        f.write(b'0'); f.flush()
    f.seek(0)
    try:
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise AlreadyRunning('Another local sync holds the lock') from exc
        yield
    finally:
        f.close()


class PublicGitHub:
    """Anonymous HTTPS only; no token is read from environment or disk."""
    def __init__(self, cache, opener=None):
        self.cache = Path(cache)
        self.cache.mkdir(parents=True, exist_ok=True)
        self.opener = opener or urllib.request.urlopen

    def open(self, url, headers=None):
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != 'https' or parsed.hostname not in ('api.github.com', 'github.com', 'release-assets.githubusercontent.com', 'objects.githubusercontent.com'):
            raise PackageError('Refusing a non-GitHub download URL')
        req = urllib.request.Request(url, headers={'User-Agent': 'or-new-public-research-sync', **(headers or {})})
        try:
            return self.opener(req, timeout=120)
        except urllib.error.HTTPError as exc:
            if exc.code == 429 or (exc.code == 403 and exc.headers.get('X-RateLimit-Remaining') == '0'):
                retry = time.time() + 1800
                for key in ('X-RateLimit-Reset',):
                    try: retry = max(retry, float(exc.headers.get(key, 0)))
                    except (TypeError, ValueError): pass
                try: retry = max(retry, time.time() + float(exc.headers.get('Retry-After', 0)))
                except (TypeError, ValueError): pass
                raise RateLimited(utc_from_epoch(retry)) from exc
            raise

    def json(self, url, ttl=1800):
        key = hashlib.sha256(url.encode()).hexdigest()
        path = self.cache / (key + '.json')
        saved = None
        if path.exists():
            saved = json.loads(path.read_text(encoding='utf-8'))
            if saved.get('url') != url or hashlib.sha256(canonical(saved['value'])).hexdigest() != saved.get('value_sha256'):
                raise PackageError('API cache integrity mismatch')
            if time.time() - saved['checked_at_epoch'] < ttl:
                return saved['value']
        headers = {'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'}
        if saved and saved.get('etag'):
            headers['If-None-Match'] = saved['etag']
        try:
            with self.open(url, headers) as response:
                raw = response.read(MAX_MANIFEST_BYTES + 1)
                if len(raw) > MAX_MANIFEST_BYTES:
                    raise PackageError('Unexpectedly large API metadata')
                value = json.loads(raw)
                etag = response.headers.get('ETag')
        except urllib.error.HTTPError as exc:
            if exc.code != 304 or saved is None:
                raise
            value, etag = saved['value'], saved.get('etag')
        atomic_json(path, {'url': url, 'checked_at_epoch': time.time(), 'etag': etag,
                           'value': value, 'value_sha256': hashlib.sha256(canonical(value)).hexdigest()})
        return value

    def pages(self, endpoint, ttl=1800):
        page = 1
        while True:
            sep = '&' if '?' in endpoint else '?'
            values = self.json(endpoint + sep + f'per_page=100&page={page}', ttl)
            if not isinstance(values, list):
                raise PackageError('Expected a paginated GitHub list')
            yield from values
            if len(values) < 100:
                return
            page += 1

    def bytes(self, url, maximum):
        with self.open(url) as response:
            value = response.read(maximum + 1)
        if len(value) > maximum:
            raise PackageError('Downloaded metadata exceeds its limit')
        return value

    def download(self, asset, path, expected_sha, expected_bytes, force=False):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if asset.get('size') != expected_bytes or asset.get('state') != 'uploaded':
            raise PackageError('Asset size/state does not match the package manifest')
        if asset.get('digest') and asset['digest'] != 'sha256:' + expected_sha:
            raise PackageError('GitHub asset digest differs from expected content')
        if path.exists() and not force:
            if path.stat().st_size == expected_bytes and sha256(path) == expected_sha:
                return
            # Preserve a corrupt cache object, but do not treat it as received.
            path.rename(path.with_name(path.name + '.invalid-' + uuid.uuid4().hex))
        temp = path.with_name(path.name + '.partial-' + uuid.uuid4().hex)
        digest, length = hashlib.sha256(), 0
        with self.open(asset['browser_download_url']) as response, temp.open('xb') as target:
            for block in iter(lambda: response.read(1024 * 1024), b''):
                length += len(block)
                if length > expected_bytes:
                    raise PackageError('Asset is larger than declared')
                target.write(block); digest.update(block)
            target.flush(); os.fsync(target.fileno())
        if length != expected_bytes or digest.hexdigest() != expected_sha:
            raise PackageError('Downloaded asset SHA/length mismatch; no receipt written')
        os.replace(temp, path)


def verify_existing(batch, m, previous=None):
    if batch.is_symlink() or not batch.is_dir():
        raise PackageError('Archive directory must be a real directory')
    expected = {f['path']: f for f in m['files']}
    found = set()
    prior_stats = (previous or {}).get('file_stats', {})
    actual_stats = {}
    for root, dirs, names in os.walk(batch, followlinks=False):
        for name in dirs + names:
            if (Path(root) / name).is_symlink():
                raise PackageError('Link appeared in immutable raw archive')
        for name in names:
            p = Path(root) / name
            rel = p.relative_to(batch).as_posix()
            if rel not in expected:
                raise PackageError('Unexpected file in immutable raw archive')
            f = expected[rel]; before = file_stat(p)
            if before[0] != f['bytes']:
                raise PackageError('Archived file size changed')
            if list(before) != prior_stats.get(rel) and sha256(p) != f['sha256']:
                raise PackageError('Archived file SHA changed')
            if file_stat(p) != before:
                raise PackageError('Archived file changed during verification')
            actual_stats[rel] = list(before); found.add(rel)
    if found != set(expected):
        raise PackageError('Archived original files are missing')
    run = json.loads((batch / 'run.json').read_text(encoding='utf-8'))
    run_metadata(run)
    if run['run_id'] != m['run_id'] or run['started_at_utc'] != m['started_at_utc']:
        raise PackageError('Archive run metadata does not match outer manifest')
    return actual_stats


def safe_extract(archive, stage, m):
    """Do not call ZipFile.extract: validate exact file set, type, size, CRC and SHA."""
    if stage.exists():
        raise PackageError('Extraction staging directory must be fresh')
    stage.mkdir(parents=True)
    expected = {f['path']: f for f in m['files']}
    seen = set()
    with zipfile.ZipFile(archive, 'r') as z:
        infos = z.infolist()
        if len(infos) != len(expected):
            raise PackageError('ZIP file count differs from manifest')
        for info in infos:
            safe_relative(info.filename)
            mode = info.external_attr >> 16
            if info.filename not in expected or info.filename.casefold() in seen or info.is_dir() or stat.S_IFMT(mode) != stat.S_IFREG or info.flag_bits & 1:
                raise PackageError('ZIP duplicate/link/nonregular/encrypted entry')
            seen.add(info.filename.casefold())
            f = expected[info.filename]
            if info.file_size != f['bytes'] or info.compress_type != zipfile.ZIP_STORED:
                raise PackageError('ZIP member size or compression does not match package format')
            target = stage.joinpath(*safe_relative(info.filename).parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            h, size = hashlib.sha256(), 0
            with z.open(info) as source, target.open('xb') as out:
                for block in iter(lambda: source.read(1024 * 1024), b''):
                    size += len(block)
                    if size > f['bytes']:
                        raise PackageError('Expanded member exceeds its declared size')
                    h.update(block); out.write(block)
            if size != f['bytes'] or h.hexdigest() != f['sha256']:
                raise PackageError('Original file SHA/length mismatch')
    (stage / 'raw').mkdir(exist_ok=True)
    return verify_existing(stage, m)


def receive_package(manifest_bytes, parts_dir, root, provenance=None, audit_local=False):
    """Receive already downloaded parts. No ingest is performed until this returns."""
    root, parts_dir = Path(root).resolve(), Path(parts_dir).resolve()
    m = json.loads(manifest_bytes)
    validate_manifest(m)
    rid, started = run_metadata(m)
    mh = hashlib.sha256(manifest_bytes).hexdigest()
    records = root / '同步记录'
    receipt_path = records / '下载回执' / (rid + '.json')
    prior = json.loads(receipt_path.read_text(encoding='utf-8')) if receipt_path.exists() else None
    if prior and (prior.get('manifest_sha256') != mh or prior.get('status') != 'verified_archived'):
        raise PackageError('Run ID already has a different download receipt')
    final = root / '原始数据' / started.strftime('%Y/%m/%d') / rid
    if root not in final.resolve().parents:
        raise PackageError('Archive destination escaped local root')
    if final.exists():
        stats = verify_existing(final, m) if audit_local or not prior else prior['file_stats']
    else:
        combined = parts_dir / ('archive-' + uuid.uuid4().hex + '.zip')
        h, size = hashlib.sha256(), 0
        with combined.open('xb') as out:
            for part in m['parts']:
                p = parts_dir / part['name']
                if p.stat().st_size != part['bytes'] or sha256(p) != part['sha256']:
                    raise PackageError('Part mismatch before archive assembly')
                with p.open('rb') as source:
                    for block in iter(lambda: source.read(1024 * 1024), b''):
                        out.write(block); h.update(block); size += len(block)
        if size != m['archive_bytes'] or h.hexdigest() != m['archive_sha256']:
            raise PackageError('Full ZIP SHA mismatch')
        stage = root / '同步缓存' / ('extract-' + rid + '-' + uuid.uuid4().hex)
        stats = safe_extract(combined, stage, m)
        final.parent.mkdir(parents=True, exist_ok=True)
        if final.exists():
            raise PackageError('Immutable destination appeared during extraction')
        os.rename(stage, final)
        combined.unlink()  # Local intermediate only; parts/originals remain available.
    stored = records / '包清单' / (rid + '.manifest.json')
    stored.parent.mkdir(parents=True, exist_ok=True)
    if stored.exists() and stored.read_bytes() != manifest_bytes:
        raise PackageError('Stored package manifest changed for same run')
    if not stored.exists():
        with stored.open('xb') as out:
            out.write(manifest_bytes); out.flush(); os.fsync(out.fileno())
    if not prior:
        receipt = {'status': 'verified_archived', 'run_id': rid, 'received_at_utc': utc_now(),
                   'batch': str(final), 'manifest_file': str(stored), 'manifest_sha256': mh,
                   'file_count': len(m['files']), 'file_stats': stats, 'provenance': provenance or {},
                   'collector_status': m.get('collector_status'), 'missing_required_files': m['missing_required_files'],
                   'not_an_ingestion_receipt': True, 'quality_pass_not_inferred': True}
        atomic_json(receipt_path, receipt)
    return final, receipt_path


def ingestion_signature():
    from . import ingest
    signature_fn = getattr(ingest, 'import_signature', None)
    if signature_fn:
        return signature_fn()
    module_dir = Path(__file__).resolve().parent
    paths = [module_dir / 'ingest.py', module_dir / 'native_html.py', module_dir.parents[1] / 'config' / 'table_mappings.json']
    if not all(p.is_file() for p in paths):
        raise PackageError('Ingest implementation/mapping signature is unavailable')
    return hashlib.sha256(canonical({p.name: sha256(p) for p in paths})).hexdigest()


def ingestion_database_identity(path):
    from .ingest import database_identity
    return database_identity(path)


def ingest_received(root, runner=subprocess.run, signature=None, audit_local=False, skip_run_ids=()):
    root = Path(root).resolve()
    records = root / '同步记录'
    database = root / '数据表增量' / 'observations.sqlite'
    results = []
    signature = signature or ingestion_signature()
    current_instance = ingestion_database_identity(database)
    for p in sorted((records / '下载回执').glob('*.json')):
        rec = json.loads(p.read_text(encoding='utf-8'))
        if rec.get('status') != 'verified_archived':
            continue
        rid = rec['run_id']; run_metadata({'run_id': rid, 'started_at_utc': '2000-01-01T00:00:00Z'})
        if rid in skip_run_ids:
            continue
        dest = records / '入库回执' / (rid + '.json')
        old = json.loads(dest.read_text(encoding='utf-8')) if dest.exists() else None
        if (old and old.get('status') == 'imported'
                and old.get('download_receipt_sha256') == sha256(p)
                and old.get('ingestion_signature') == signature
                and old.get('database') == str(database)
                and current_instance is not None
                and old.get('database_instance_id') == current_instance and not audit_local):
            continue
        mpath = Path(rec['manifest_file'])
        if sha256(mpath) != rec['manifest_sha256']:
            raise PackageError('Local package manifest failed before ingestion')
        m = json.loads(mpath.read_bytes())
        validate_manifest(m)
        rid, start = run_metadata(m)
        expected_batch = root / '原始数据' / start.strftime('%Y/%m/%d') / rid
        if Path(rec['batch']).resolve() != expected_batch.resolve() or root not in expected_batch.resolve().parents:
            raise PackageError('Download receipt archive path mismatch')
        if audit_local:
            verify_existing(expected_batch, m)
        started = utc_now()
        attempt = uuid.uuid4().hex
        logs = records / '入库日志'; logs.mkdir(parents=True, exist_ok=True)
        out_path, err_path = logs / (rid + '-' + attempt + '.out.log'), logs / (rid + '-' + attempt + '.err.log')
        database.parent.mkdir(parents=True, exist_ok=True)
        if rec.get('missing_required_files'):
            code, error = None, 'Collector bundle lacks required files; archived but ingestion deferred'
        else:
            try:
                with out_path.open('wb') as out, err_path.open('wb') as err:
                    proc = runner([sys.executable, '-m', 'or_pipeline.ingest', '--batch', rec['batch'], '--database', str(database)], stdout=out, stderr=err, check=False)
                code, error = proc.returncode, None
            except OSError as exc:
                code, error = None, str(exc)
        if code == 0:
            try:
                after_instance = ingestion_database_identity(database)
                if after_instance is None:
                    raise PackageError('Ingest returned success without an observation database instance')
                if current_instance is not None and current_instance != after_instance:
                    raise PackageError('Observation database instance changed during ingestion')
                current_instance = after_instance
            except Exception as exc:
                code, error = None, str(exc)
        result = {'status': 'imported' if code == 0 else 'import_failed', 'run_id': rid,
                  'started_at_utc': started, 'finished_at_utc': utc_now(), 'returncode': code,
                  'download_receipt_sha256': sha256(p), 'database': str(database),
                  'ingestion_signature': signature,
                  'database_instance_id': current_instance if code == 0 else None,
                  'stdout_file': str(out_path), 'stderr_file': str(err_path), 'error': error,
                  'collector_quality_pass_not_inferred': True}
        # Every failed attempt remains; the per-run pointer supports retry/idempotency.
        atomic_json(records / '入库尝试' / (rid + '-' + attempt + '.json'), result)
        atomic_json(dest, result)
        results.append(result)
    return results


def asset_fingerprint(assets, names):
    result = {}
    for name in names:
        a = assets.get(name)
        if a is None or a.get('id') is None or not a.get('updated_at') or type(a.get('size')) is not int or a.get('state') != 'uploaded' or not a.get('browser_download_url'):
            return None
        result[name] = {k: a.get(k) for k in ('id', 'updated_at', 'size', 'digest', 'state', 'browser_download_url')}
    return result


def cached_receipt(root, name, assets, release, audit_local=False):
    rid = name[3:-len('.manifest.json')]
    try: safe_relative(rid)
    except PackageError: return None
    records = root / '同步记录'
    p = records / '下载回执' / (rid + '.json')
    check = records / '云端核查' / (rid + '.json')
    if not p.exists() or not check.exists():
        return None
    r, c = json.loads(p.read_text(encoding='utf-8')), json.loads(check.read_text(encoding='utf-8'))
    if r.get('status') != 'verified_archived' or c.get('download_receipt_sha256') != sha256(p):
        return None
    saved = Path(r['manifest_file'])
    if not saved.is_file() or sha256(saved) != r['manifest_sha256']:
        raise PackageError('Saved manifest changed for an already received run')
    m = json.loads(saved.read_bytes()); validate_manifest(m)
    names = [x['name'] for x in m['parts']] + [name, name[:-5] + '.sha256']
    fp = asset_fingerprint(assets, names)
    if fp is None or c.get('asset_fingerprint') != fp or c.get('release_id') != release['id'] or m['release_tag'] != release['tag_name']:
        return None
    batch = Path(r['batch'])
    if not batch.is_dir() or batch.is_symlink() or not (batch / 'run.json').is_file():
        return None
    if audit_local:
        verify_existing(batch, m)
    return rid


def sync(repository, root, client=None, do_ingest=True, audit_local=False):
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
        raise PackageError('Invalid public repository')
    root = Path(root).resolve(); records = root / '同步记录'; records.mkdir(parents=True, exist_ok=True)
    with sync_lock(records / 'sync.lock'):
        imports = ingest_received(root, audit_local=audit_local) if do_ingest else []
        state_file = records / '最近同步.json'
        previous = json.loads(state_file.read_text(encoding='utf-8')) if state_file.exists() else {}
        retry = previous.get('next_retry_at_utc')
        if retry and dt.datetime.fromisoformat(retry.replace('Z', '+00:00')) > dt.datetime.now(dt.timezone.utc):
            return {'status': 'rate_limited_waiting', 'next_retry_at_utc': retry, 'ingestion_attempts': len(imports)}
        client = client or PublicGitHub(records / 'HTTP缓存')
        result = {'status': 'running', 'started_at_utc': utc_now(), 'repository': repository, 'received_runs': [], 'already_received_runs': [], 'issues': [], 'releases_scanned': 0, 'ingestion_attempts': len(imports)}
        try:
            endpoint = f'https://api.github.com/repos/{repository}/releases'
            for release in client.pages(endpoint):
                tag = release.get('tag_name', '')
                if not RELEASE.fullmatch(tag) or release.get('draft') or release.get('prerelease'):
                    continue
                ttl = 1800 if tag.endswith(dt.datetime.now(dt.timezone.utc).strftime('%Y-%m')) else 21600
                asset_rows = list(client.pages(f'{endpoint}/{release["id"]}/assets', ttl=ttl))
                assets = {a['name']: a for a in asset_rows}
                if len(assets) != len(asset_rows):
                    raise PackageError('Duplicate asset names in release')
                result['releases_scanned'] += 1
                for name, asset in assets.items():
                    if not (name.startswith('or-') and name.endswith('.manifest.json')):
                        continue
                    try:
                        cached = cached_receipt(root, name, assets, release, audit_local)
                        if cached:
                            result['already_received_runs'].append(cached)
                            continue
                        side_name = name[:-5] + '.sha256'
                        if side_name not in assets:
                            raise PackageError('Manifest publication has no checksum sidecar yet')
                        mb = client.bytes(asset['browser_download_url'], MAX_MANIFEST_BYTES)
                        side = client.bytes(assets[side_name]['browser_download_url'], 1024).decode('ascii').strip()
                        mh = hashlib.sha256(mb).hexdigest()
                        if side != mh + '  ' + name:
                            raise PackageError('Manifest sidecar mismatch')
                        m = json.loads(mb); validate_manifest(m)
                        if name != 'or-' + m['run_id'] + '.manifest.json' or m['release_tag'] != tag:
                            raise PackageError('Run/filename/release binding mismatch')
                        cache = root / '同步缓存' / m['run_id']; cache.mkdir(parents=True, exist_ok=True)
                        receipt = records / '下载回执' / (m['run_id'] + '.json')
                        names = [x['name'] for x in m['parts']] + [name, side_name]
                        fp = asset_fingerprint(assets, names)
                        if fp is None:
                            raise PackageError('A package part has not been published')
                        for part in m['parts']:
                            client.download(assets[part['name']], cache / part['name'], part['sha256'], part['bytes'], force=receipt.exists() and not assets[part['name']].get('digest'))
                        existed = receipt.exists()
                        batch, rp = receive_package(mb, cache, root, {'repository': repository, 'release_tag': tag, 'release_id': release['id'], 'manifest_asset_id': asset['id'], 'manifest_url': asset['browser_download_url'], 'asset_fingerprint': fp}, audit_local=audit_local)
                        atomic_json(records / '云端核查' / (m['run_id'] + '.json'), {'verified_at_utc': utc_now(), 'download_receipt_sha256': sha256(rp), 'manifest_sha256': mh, 'release_id': release['id'], 'asset_fingerprint': fp})
                        result['already_received_runs' if existed else 'received_runs'].append(m['run_id'])
                    except RateLimited:
                        raise
                    except Exception as exc:
                        result['issues'].append({'release_tag': tag, 'asset': name, 'error': str(exc)})
            result['status'] = 'complete' if not result['issues'] else 'partial'
        except RateLimited as exc:
            result.update(status='rate_limited', next_retry_at_utc=exc.retry_at)
        except Exception as exc:
            result['status'] = 'failed'; result['issues'].append({'error': str(exc)})
        if do_ingest:
            imported = ingest_received(root, skip_run_ids={r['run_id'] for r in imports}); imports.extend(imported); result['ingestion_attempts'] += len(imported)
            if any(r['status'] != 'imported' for r in imports) and result['status'] == 'complete':
                result['status'] = 'partial'
        result['finished_at_utc'] = utc_now()
        atomic_json(records / '同步尝试' / (uuid.uuid4().hex + '.json'), result)
        atomic_json(state_file, result)
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', default='lyll23/or_new')
    parser.add_argument('--root', required=True)
    parser.add_argument('--download-only', action='store_true', help='Archive verified originals without invoking ingest')
    parser.add_argument('--audit-local', action='store_true', help='Explicitly rehash all archived original files during this sync')
    args = parser.parse_args(argv)
    try:
        result = sync(args.repository, args.root, do_ingest=not args.download_only, audit_local=args.audit_local)
    except AlreadyRunning:
        result = {'status': 'another_sync_in_progress'}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result['status'] in ('complete', 'another_sync_in_progress') else 2


if __name__ == '__main__':
    raise SystemExit(main())
