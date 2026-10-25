import hashlib,json,os,pathlib,sys,tarfile
root=pathlib.Path('/seed')
expected_sha=sys.argv[1]
seen=set()
expected=None
with tarfile.open(fileobj=sys.stdin.buffer,mode='r|') as archive:
    for member in archive:
        name=member.name
        relative=pathlib.PurePosixPath(name)
        if not member.isfile() or member.islnk() or member.issym() or relative.is_absolute() or '..' in relative.parts or name in seen:
            raise RuntimeError('invalid_candidate_archive')
        seen.add(name)
        stream=archive.extractfile(member)
        if expected is None:
            if name!='manifest.json' or member.size>1024*1024:
                raise RuntimeError('manifest_must_be_first')
            raw=stream.read()
            if hashlib.sha256(raw).hexdigest()!=expected_sha:
                raise RuntimeError('manifest_pin_mismatch')
            manifest=json.loads(raw)
            if manifest['status']!='candidate' or manifest['review_status']!='not_reviewed':
                raise RuntimeError('unexpected_candidate_state')
            expected={entry['path']:entry for entry in manifest['files']}
            if len(expected)!=len(manifest['files']):
                raise RuntimeError('duplicate_candidate_path')
            for entry in manifest['files']:
                path=pathlib.PurePosixPath(entry['path'])
                if path.is_absolute() or '..' in path.parts or path.parts[0] not in ('data','models'):
                    raise RuntimeError('invalid_candidate_path')
            with (root/'manifest.json').open('xb') as target:
                target.write(raw)
            continue
        if name not in expected or member.size!=expected[name]['size_bytes']:
            raise RuntimeError('candidate_membership_mismatch')
        target=root/relative
        target.parent.mkdir(parents=True,exist_ok=True)
        digest=hashlib.sha256()
        with target.open('xb') as output:
            for chunk in iter(lambda:stream.read(1024*1024),b''):
                output.write(chunk)
                digest.update(chunk)
            output.flush()
            os.fchmod(output.fileno(),expected[name]['mode'] & 0o777)
        if digest.hexdigest()!=expected[name]['sha256']:
            raise RuntimeError('candidate_content_mismatch')
if expected is None or seen!=set(expected)|{'manifest.json'}:
    raise RuntimeError('candidate_archive_incomplete')
for name,entry in expected.items():
    digest=hashlib.sha256()
    with (root/name).open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''): digest.update(chunk)
    if digest.hexdigest()!=entry['sha256']:
        raise RuntimeError('candidate_seed_hash_mismatch')
with (root/'.seed-complete').open('x') as target:
    target.write(expected_sha+'\n')
print('candidate_seed=verified files='+str(len(expected)))
