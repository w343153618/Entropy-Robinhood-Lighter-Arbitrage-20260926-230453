"""The local SDK dependency repair must leave Python and native bytes intact."""
import base64
import csv
import hashlib
import io
import json
import subprocess
import sys
import zipfile

import pytest

from scripts.build_lighter_wheel import patch_wheel, verify_archive


def wheel_fixture(path, constraint='urllib3<2.1.0,>=1.25.3', corrupt=False):
    files = {'lighter/client.py': b'print("unchanged SDK")\n',
             'lighter/signers/native.so': b'\x00\x01\x02opaque-native-payload',
             'lighter_sdk-1.1.4.dist-info/METADATA':
             f'Name: lighter-sdk\nVersion: 1.1.4\nRequires-Dist: {constraint}\n'.encode()}
    record = 'lighter_sdk-1.1.4.dist-info/RECORD'
    out = io.StringIO(newline='')
    writer = csv.writer(out, lineterminator='\n')
    for name, data in files.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()
        writer.writerow([name, 'sha256=' + digest, str(len(data))])
    writer.writerow([record, '', ''])
    files[record] = out.getvalue().encode()
    if corrupt:
        files['lighter/client.py'] += b'corrupted'
    with zipfile.ZipFile(path, 'w') as wheel:
        for name, data in files.items():
            wheel.writestr(name, data)


def test_metadata_repair_is_reproducible_and_preserves_all_payloads(tmp_path):
    source, first, second = [tmp_path / f'{name}.whl' for name in ('source', 'first', 'second')]
    wheel_fixture(source)
    a, b = patch_wheel(source, first), patch_wheel(source, second)
    assert a == b
    assert first.read_bytes() == second.read_bytes()
    assert a['patched_sha256'] == hashlib.sha256(first.read_bytes()).hexdigest()
    with zipfile.ZipFile(source) as original, zipfile.ZipFile(first) as patched:
        assert original.namelist() == patched.namelist()
        for name in original.namelist():
            if name.endswith(('METADATA', 'RECORD')):
                continue
            assert original.read(name) == patched.read(name)
        metadata = patched.read('lighter_sdk-1.1.4.dist-info/METADATA')
        assert b'Requires-Dist: urllib3<3,>=2.7.0\n' in metadata
    assert len(a['changed_files']) == 2
    json.dumps(a)


@pytest.mark.parametrize('constraint,corrupt', [
    ('urllib3>=2.7.0', False), ('urllib3<2.1.0,>=1.25.3', True)])
def test_unknown_metadata_or_corrupt_payload_fails_closed(tmp_path, constraint, corrupt):
    original, destination = tmp_path / 'original.whl', tmp_path / 'patched.whl'
    wheel_fixture(original, constraint, corrupt)
    with pytest.raises(ValueError):
        patch_wheel(original, destination)
    assert not destination.exists()


def test_archive_checksum_rejects_changed_source_before_build(tmp_path):
    source = tmp_path / 'source.tar.gz'
    source.write_bytes(b'not the pinned official archive')
    with pytest.raises(ValueError):
        verify_archive(source)


def test_cli_bad_archive_exits_nonzero_without_install_or_download(tmp_path):
    archive = tmp_path / 'bad.tar.gz'
    archive.write_bytes(b'wrong')
    completed = subprocess.run([sys.executable, 'scripts/build_lighter_wheel.py',
                                '--source-archive', str(archive), '--output-dir',
                                str(tmp_path / 'out')], capture_output=True, text=True, check=False)
    assert completed.returncode != 0
    assert 'checksum' in completed.stderr.lower()
    assert not list((tmp_path / 'out').glob('*.whl'))
