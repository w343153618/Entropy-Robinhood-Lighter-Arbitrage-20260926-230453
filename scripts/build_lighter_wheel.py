#!/usr/bin/env python3
"""Build the pinned official SDK, repairing only its obsolete urllib3 metadata.

No SDK Python or native signer bytes are changed. The original source checksum,
original wheel checksum, repair and payload comparison are recorded alongside the
wheel. Run this before installing requirements-live.txt. No credentials are used.
"""
from __future__ import annotations

import argparse
import base64
import copy
import csv
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path

COMMIT = 'a38b6405f362fc14a562fe7a97df03f3ee756bc1'
SOURCE_URL = f'https://codeload.github.com/elliottech/lighter-python/tar.gz/{COMMIT}'
SOURCE_SHA256 = '8f7fddb7a887bbf81450a08a5dd2366f5a79ccd8d23807d390ef179fc859e8f6'
OLD_CONSTRAINT = b'Requires-Dist: urllib3<2.1.0,>=1.25.3\n'
NEW_CONSTRAINT = b'Requires-Dist: urllib3<3,>=2.7.0\n'
WHEEL_NAME = 'lighter_sdk-1.1.4-py3-none-any.whl'
SOURCE_DATE_EPOCH = '1700000000'
BUILD_REQUIREMENTS = ['setuptools==83.0.0', 'wheel==0.46.3', 'packaging==26.3']
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024


def sha256(path: Path) -> str:
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def verify_archive(path: Path) -> None:
    if path.stat().st_size > MAX_ARCHIVE_BYTES or sha256(path) != SOURCE_SHA256:
        raise ValueError('official source checksum mismatch; refusing to build')


def _verify_record(entries: dict[str, bytes], record: str) -> list[list[str]]:
    rows = list(csv.reader(io.StringIO(entries[record].decode())))
    if any(len(row) != 3 for row in rows) or len({row[0] for row in rows}) != len(rows):
        raise ValueError('invalid or duplicate wheel RECORD entries')
    if {row[0] for row in rows} != set(entries):
        raise ValueError('wheel payload is not completely described by RECORD')
    for name, digest, size in rows:
        if name == record:
            if digest or size:
                raise ValueError('invalid RECORD self-entry')
            continue
        data = entries[name]
        expected = 'sha256=' + base64.urlsafe_b64encode(
            hashlib.sha256(data).digest()).rstrip(b'=').decode()
        if digest != expected or size != str(len(data)):
            raise ValueError(f'wheel RECORD checksum mismatch: {name}')
    return rows


def patch_wheel(original: Path, destination: Path) -> dict:
    """Repair the exact known metadata line and validate all original payloads."""
    with zipfile.ZipFile(original) as source:
        names = source.namelist()
        if len(set(names)) != len(names) or any(
                Path(name).is_absolute() or '..' in Path(name).parts for name in names):
            raise ValueError('unsafe or duplicate wheel entry')
        entries = {name: source.read(name) for name in names}
        metadata = 'lighter_sdk-1.1.4.dist-info/METADATA'
        record = 'lighter_sdk-1.1.4.dist-info/RECORD'
        if metadata not in entries or record not in entries:
            raise ValueError('unexpected SDK distribution metadata')
        rows = _verify_record(entries, record)
        if entries[metadata].count(OLD_CONSTRAINT) != 1:
            raise ValueError('unexpected urllib3 constraint; refusing to patch')
        entries[metadata] = entries[metadata].replace(OLD_CONSTRAINT, NEW_CONSTRAINT)
        for row in rows:
            if row[0] == metadata:
                payload = entries[metadata]
                row[1] = 'sha256=' + base64.urlsafe_b64encode(
                    hashlib.sha256(payload).digest()).rstrip(b'=').decode()
                row[2] = str(len(payload))
        output = io.StringIO(newline='')
        csv.writer(output, lineterminator='\n').writerows(rows)
        entries[record] = output.getvalue().encode()
        _verify_record(entries, record)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix('.whl.tmp')
        try:
            with zipfile.ZipFile(temporary, 'w', compression=zipfile.ZIP_DEFLATED,
                                 compresslevel=9) as patched:
                for info in source.infolist():
                    info = copy.copy(info)
                    # Fix timestamps even for a fixture or externally supplied
                    # original wheel; never inherit wall-clock build times.
                    info.date_time = (2023, 11, 14, 22, 13, 20)
                    patched.writestr(info, entries[info.filename])
            with zipfile.ZipFile(temporary) as patched:
                if patched.namelist() != names:
                    raise ValueError('wheel entry list changed')
                changed = [name for name in names if source.read(name) != patched.read(name)]
                if set(changed) != {metadata, record}:
                    raise ValueError('SDK Python or native signer payload changed')
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)
    return {'original_sha256': sha256(original), 'patched_sha256': sha256(destination),
            'changed_files': changed, 'old_constraint': OLD_CONSTRAINT.decode().strip(),
            'new_constraint': NEW_CONSTRAINT.decode().strip()}


def _download(destination: Path) -> None:
    with urllib.request.urlopen(SOURCE_URL, timeout=30) as response, destination.open('wb') as out:
        total = 0
        while chunk := response.read(1024 * 1024):
            total += len(chunk)
            if total > MAX_ARCHIVE_BYTES:
                raise ValueError('official source archive exceeded size limit')
            out.write(chunk)


def build(source_archive: Path | None, output_dir: Path) -> dict:
    with tempfile.TemporaryDirectory(prefix='entropy-lighter-build-') as temporary:
        work = Path(temporary)
        archive = source_archive or work / 'official-source.tar.gz'
        if source_archive is None:
            _download(archive)
        verify_archive(archive)  # Must precede extraction, pip and setup execution.
        source = work / 'source'
        source.mkdir()
        with tarfile.open(archive) as tar:
            members = tar.getmembers()
            if any(not member.isfile() and not member.isdir() for member in members) \
                    or sum(member.size for member in members) > 200 * 1024 * 1024:
                raise ValueError('unsupported source archive contents')
            tar.extractall(source, filter='data')
        source_root = source / f'lighter-python-{COMMIT}'
        if not (source_root / 'setup.py').is_file():
            raise ValueError('official source root missing')
        env_path = work / 'build-env'
        subprocess.run([sys.executable, '-m', 'venv', str(env_path)], check=True, timeout=60)
        python = env_path / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
        subprocess.run([str(python), '-m', 'pip', 'install', '--disable-pip-version-check',
                        *BUILD_REQUIREMENTS], check=True, timeout=180)
        build_env = {**os.environ, 'SOURCE_DATE_EPOCH': SOURCE_DATE_EPOCH,
                     'PYTHONHASHSEED': '0'}
        proofs = []
        for attempt in (1, 2):
            wheels = work / f'original{attempt}'
            subprocess.run([str(python), '-m', 'pip', 'wheel', '--no-deps',
                            '--no-build-isolation', '--disable-pip-version-check',
                            '--wheel-dir', str(wheels), str(source_root)],
                           env=build_env, check=True, timeout=180)
            original = wheels / WHEEL_NAME
            if list(wheels.glob('*.whl')) != [original]:
                raise ValueError('unexpected SDK build artifact')
            proofs.append(patch_wheel(original, work / f'patched{attempt}' / WHEEL_NAME))
        if proofs[0] != proofs[1]:
            raise ValueError('repeated builds produced different checksums')
        proof = {**proofs[0], 'source_commit': COMMIT, 'source_url': SOURCE_URL,
                 'source_sha256': SOURCE_SHA256, 'source_date_epoch': SOURCE_DATE_EPOCH,
                 'build_requirements': BUILD_REQUIREMENTS,
                 'python': sys.version.split()[0], 'payload_invariant': True,
                 'repeated_builds_identical': True}
        output_dir.mkdir(parents=True, exist_ok=True)
        destination = output_dir / WHEEL_NAME
        with (work / 'patched1' / WHEEL_NAME).open('rb') as wheel, \
                destination.with_suffix('.whl.tmp').open('wb') as out:
            while chunk := wheel.read(1024 * 1024):
                out.write(chunk)
        destination.with_suffix('.whl.tmp').replace(destination)
        provenance = output_dir / 'lighter-sdk-provenance.json'
        provenance.with_suffix('.json.tmp').write_text(json.dumps(proof, indent=2) + '\n')
        provenance.with_suffix('.json.tmp').replace(provenance)
        return proof


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-archive', type=Path,
                        help='Use an already downloaded archive; checksum is still mandatory')
    parser.add_argument('--output-dir', type=Path, default=Path('.wheelhouse'))
    args = parser.parse_args()
    try:
        proof = build(args.source_archive, args.output_dir)
    except (OSError, ValueError, zipfile.BadZipFile, tarfile.TarError,
            subprocess.SubprocessError) as exc:
        print(f'SDK wheel build failed: {exc}', file=sys.stderr)
        return 1
    print(json.dumps(proof, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
