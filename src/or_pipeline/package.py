"""Immutable run packages and monthly public GitHub Release publication.

Release limits: https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases
Cloud publication uses the GitHub CLI already installed on GitHub-hosted runners.
Local package creation and verification use the Python standard library only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import tempfile
import urllib.request
import zipfile

SCHEMA = 'or-new-run-package-v1'
PART_BYTES = 512 * 1024 * 1024
ASSET_LIMIT = 999  # Strictly below GitHub's 1000 assets/release ceiling.
REQUIRED = {'run.json', 'catalog.json', 'plan.json', 'manifest.jsonl', 'quality.json'}
RUN_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z')
HASH = re.compile(r'[0-9a-f]{64}\Z')
RESERVED = {'CON', 'PRN', 'AUX', 'NUL', *(f'COM{i}' for i in range(1, 10)), *(f'LPT{i}' for i in range(1, 10))}


class PackageError(ValueError):
    pass


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z')


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def file_stat(path):
    s = Path(path).stat()
    return tuple(getattr(s, k) for k in ('st_size', 'st_mtime_ns', 'st_ctime_ns', 'st_ino', 'st_dev'))


def safe_relative(name):
    if not isinstance(name, str) or not name or '\\' in name or ':' in name or name.startswith('/'):
        raise PackageError('Unsafe relative path')
    parts = name.split('/')
    if any(not p or p in ('.', '..') or p[-1:] in (' ', '.') or any(ord(c) < 32 for c in p)
           or p.split('.')[0].upper() in RESERVED for p in parts):
        raise PackageError('Unsafe path component: ' + name)
    return PurePosixPath(name)


def run_metadata(run):
    rid = run.get('run_id')
    if not isinstance(rid, str) or not RUN_ID.fullmatch(rid):
        raise PackageError('run_id must be a safe stable identifier')
    safe_relative(rid)
    try:
        start = dt.datetime.fromisoformat(run['started_at_utc'].replace('Z', '+00:00'))
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise PackageError('A timezone-aware started_at_utc is required') from exc
    if start.tzinfo is None or start.utcoffset() != dt.timedelta(0):
        raise PackageError('started_at_utc must explicitly identify UTC')
    return rid, start.astimezone(dt.timezone.utc)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp')
    with temp.open('wb') as stream:
        stream.write(canonical(value) + b'\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


def build(batch, out, part_bytes=PART_BYTES):
    batch, out = Path(batch).resolve(), Path(out).resolve()
    if not 1 <= part_bytes < 2 * 1024**3:
        raise PackageError('Each part must be strictly smaller than 2 GiB')
    if out == batch or batch in out.parents:
        raise PackageError('Package output must be outside collector input')
    run_bytes = (batch / 'run.json').read_bytes()
    run = json.loads(run_bytes)
    rid, start = run_metadata(run)
    prefix = 'or-' + rid
    manifest_path = out / (prefix + '.manifest.json')
    if out.exists() and any(out.iterdir()):
        raise PackageError('Package output must be fresh; use the existing manifest to retry publication')
    out.mkdir(parents=True, exist_ok=True)
    files, seen = [], set()
    for root, dirs, names in os.walk(batch, followlinks=False):
        for name in dirs + names:
            if (Path(root) / name).is_symlink():
                raise PackageError('Symlinks are not allowed in collector output')
        for name in names:
            p = Path(root) / name
            rel = p.relative_to(batch).as_posix()
            safe_relative(rel)
            if rel.casefold() in seen or not p.is_file():
                raise PackageError('Duplicate or nonregular source file')
            seen.add(rel.casefold())
            before = file_stat(p)
            digest = sha256(p)
            if before != file_stat(p):
                raise PackageError('Collector input changed while hashing')
            files.append({'path': rel, 'bytes': before[0], 'sha256': digest, '_stat': before})
    files.sort(key=lambda row: row['path'])
    if next(f['sha256'] for f in files if f['path'] == 'run.json') != hashlib.sha256(run_bytes).hexdigest():
        raise PackageError('Run metadata changed before packaging')
    missing = sorted(REQUIRED - {row['path'] for row in files})
    archive = out / (prefix + '.zip.building')
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_STORED, allowZip64=True) as z:
        for f in files:
            p = batch / f['path']
            info = zipfile.ZipInfo(f['path'], (1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o644) << 16
            h = hashlib.sha256()
            with p.open('rb') as source, z.open(info, 'w', force_zip64=True) as target:
                for block in iter(lambda: source.read(1024 * 1024), b''):
                    h.update(block)
                    target.write(block)
            if h.hexdigest() != f['sha256'] or file_stat(p) != f['_stat']:
                raise PackageError('Collector input changed while packaging')
    archive_sha = sha256(archive)
    parts = []
    with archive.open('rb') as source:
        index = 1
        while True:
            first = source.read(min(part_bytes, 1024 * 1024))
            if not first:
                break
            name = f'{prefix}.zip.part{index:04d}'
            h, size = hashlib.sha256(), 0
            with (out / name).open('xb') as target:
                block = first
                while block:
                    target.write(block)
                    h.update(block)
                    size += len(block)
                    block = source.read(min(part_bytes - size, 1024 * 1024)) if size < part_bytes else b''
            parts.append({'name': name, 'bytes': size, 'sha256': h.hexdigest()})
            index += 1
    for f in files:
        if file_stat(batch / f['path']) != f.pop('_stat'):
            raise PackageError('Collector source changed before package closure')
    manifest = {'schema': SCHEMA, 'run_id': rid, 'started_at_utc': run['started_at_utc'],
                'release_tag': start.strftime('research-data-%Y-%m'), 'packaged_at_utc': utc_now(),
                'archive_name': prefix + '.zip', 'archive_bytes': archive.stat().st_size,
                'archive_sha256': archive_sha, 'parts': parts, 'files': files,
                'missing_required_files': missing, 'collector_status': run.get('status'),
                'quality_is_not_certified_by_packaging': True}
    validate_manifest(manifest)
    atomic_json(manifest_path, manifest)
    sidecar = manifest_path.with_suffix('.sha256')
    sidecar.write_text(sha256(manifest_path) + '  ' + manifest_path.name + '\n', encoding='ascii')
    archive.unlink()  # Only our intermediate ZIP; published parts and originals are retained.
    return manifest_path


def validate_manifest(m):
    if m.get('schema') != SCHEMA:
        raise PackageError('Unsupported package schema')
    rid, start = run_metadata(m)
    if m.get('release_tag') != start.strftime('research-data-%Y-%m'):
        raise PackageError('Release month does not match UTC run start')
    names, folded, total = set(), set(), 0
    parts = m.get('parts')
    if not isinstance(parts, list) or not parts or len(parts) + 2 > ASSET_LIMIT:
        raise PackageError('Invalid part count or release asset capacity')
    for i, p in enumerate(parts, 1):
        expected = f'or-{rid}.zip.part{i:04d}'
        if p.get('name') != expected or type(p.get('bytes')) is not int or not 0 < p['bytes'] < 2 * 1024**3 or not HASH.fullmatch(p.get('sha256', '')):
            raise PackageError('Invalid part declaration')
        total += p['bytes']
    if total != m.get('archive_bytes') or not HASH.fullmatch(m.get('archive_sha256', '')):
        raise PackageError('Invalid full ZIP declaration')
    if not isinstance(m.get('files'), list) or not m['files']:
        raise PackageError('Missing original file inventory')
    for f in m['files']:
        safe_relative(f.get('path'))
        if f['path'].casefold() in folded or type(f.get('bytes')) is not int or f['bytes'] < 0 or not HASH.fullmatch(f.get('sha256', '')):
            raise PackageError('Invalid or duplicate file declaration')
        names.add(f['path'])
        folded.add(f['path'].casefold())
    if 'run.json' not in names or m.get('missing_required_files') != sorted(REQUIRED - names):
        raise PackageError('Required-file status is inconsistent')


def gh(args):
    r = subprocess.run(['gh', *args], text=True, encoding='utf-8', capture_output=True)
    if r.returncode:
        raise PackageError('GitHub command failed: ' + r.stderr.strip())
    return r.stdout


def remote_digest(url):
    h = hashlib.sha256()
    with urllib.request.urlopen(url, timeout=120) as response:
        for block in iter(lambda: response.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def publish(manifest_path, repository):
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
        raise PackageError('Invalid repository')
    manifest_path = Path(manifest_path).resolve()
    m = json.loads(manifest_path.read_text(encoding='utf-8'))
    validate_manifest(m)
    sidecar = manifest_path.with_suffix('.sha256')
    if sidecar.read_text(encoding='ascii').strip() != sha256(manifest_path) + '  ' + manifest_path.name:
        raise PackageError('Manifest SHA sidecar mismatch')
    assets = [(manifest_path.parent / p['name'], p['sha256'], p['bytes']) for p in m['parts']]
    assets += [(sidecar, sha256(sidecar), sidecar.stat().st_size), (manifest_path, sha256(manifest_path), manifest_path.stat().st_size)]
    for p, h, size in assets:
        if p.stat().st_size != size or sha256(p) != h:
            raise PackageError('Package asset changed before publication')
    tag = m['release_tag']
    releases = json.loads(gh(['api', f'repos/{repository}/releases?per_page=100', '--paginate', '--slurp']))
    matches = [r for page in releases for r in page if r['tag_name'] == tag]
    if not matches:
        gh(['release', 'create', tag, '--repo', repository, '--title', tag,
            '--notes', 'Public research run packages. Each manifest is published after all of its parts. No automatic cloud deletion.', '--latest=false'])
        release = json.loads(gh(['api', f'repos/{repository}/releases/tags/{tag}']))
    elif len(matches) == 1:
        release = matches[0]
    else:
        raise PackageError('Ambiguous monthly release')
    if release['draft'] or release['prerelease']:
        raise PackageError('Research buffering requires an ordinary published release')
    existing_pages = json.loads(gh(['api', f'repos/{repository}/releases/{release["id"]}/assets?per_page=100', '--paginate', '--slurp']))
    existing = {a['name']: a for page in existing_pages for a in page}
    added = [p.name for p, _, _ in assets if p.name not in existing]
    if len(existing) + len(added) > ASSET_LIMIT:
        raise PackageError('Monthly Release asset ceiling reached; nothing silently dropped or deleted. Create an explicit revised buffering plan.')
    uploaded = []
    for p, h, size in assets:  # Manifest last is the publication commit.
        if p.name in existing:
            a = existing[p.name]
            got = a.get('digest')
            if a.get('state') != 'uploaded' or a['size'] != size or (got != 'sha256:' + h and remote_digest(a['browser_download_url']) != h):
                raise PackageError('Existing cloud asset differs; immutable publication refuses overwrite: ' + p.name)
        else:
            gh(['release', 'upload', tag, str(p), '--repo', repository])
        uploaded.append(p.name)
    return {'status': 'published', 'repository': repository, 'release_tag': tag, 'run_id': m['run_id'], 'assets': uploaded, 'manifest_sha256': sha256(manifest_path)}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    b = sub.add_parser('build'); b.add_argument('--batch', required=True); b.add_argument('--out', required=True); b.add_argument('--part-bytes', type=int, default=PART_BYTES)
    u = sub.add_parser('publish'); u.add_argument('--manifest', required=True); u.add_argument('--repository', required=True)
    a = p.parse_args(argv)
    if a.command == 'build':
        print(str(build(a.batch, a.out, a.part_bytes)))
    else:
        print(json.dumps(publish(a.manifest, a.repository), ensure_ascii=False))


if __name__ == '__main__':
    main()
