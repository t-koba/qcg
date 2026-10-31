#!/usr/bin/env python3
"""Rebuild the local fork from a verified upstream tarball; never fetch anything."""
import base64
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tarfile
import tempfile

vendor = Path(__file__).resolve().parent
if len(sys.argv) != 3:
    sys.exit("usage: build-openapi-core.py UPSTREAM.tgz OUTPUT.tgz")
upstream = (vendor / "openapi-core-upstream.json").read_bytes()
expected = json.loads(upstream)["integrity"]
raw = Path(sys.argv[1]).read_bytes()
if "sha512-" + base64.b64encode(hashlib.sha512(raw).digest()).decode() != expected:
    sys.exit("upstream integrity mismatch")
with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as archive:
        for member in archive:
            path = PurePosixPath(member.name)
            if not member.isfile() or path.parts[0] != "package" or ".." in path.parts:
                sys.exit("unexpected upstream tar entry")
            target = root.joinpath(*path.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.extractfile(member).read())
            target.chmod(member.mode & 0o777)
    package = root / "package"
    subprocess.run(
        ["patch", "--batch", "--fuzz=0", "-p1", "-i", str(vendor / "openapi-core.patch")],
        cwd=package, check=True,
    )
    (package / "QCG-UPSTREAM.json").write_bytes(upstream)
    (package / "QCG-PATCH.diff").write_bytes((vendor / "openapi-core.patch").read_bytes())
    # Canonical order, ownership, timestamps and gzip header, independent of checkout.
    with Path(sys.argv[2]).open("wb") as output:
        with gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as archive:
                for path in sorted(package.rglob("*")):
                    if path.is_file():
                        entry = tarfile.TarInfo(path.relative_to(root).as_posix())
                        content = path.read_bytes()
                        entry.size = len(content)
                        entry.mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
                        archive.addfile(entry, io.BytesIO(content))
print("sha512-" + base64.b64encode(hashlib.sha512(Path(sys.argv[2]).read_bytes()).digest()).decode())
