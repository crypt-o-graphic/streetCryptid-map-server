#!/usr/bin/env python3
"""Checkpointed Planetiler builds and independently retryable S3 imports."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import shutil
import signal
import struct
import subprocess
import sys
import threading
import tempfile
import time
import uuid

LOG = logging.getLogger("planet-bake")
JAR = Path(os.environ.get("PLANETILER_JAR", "/app/planetiler.jar"))
PART_SIZE = 64 << 20
MAX_JSON = 8 << 20
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,119}\Z")
CHUNK = re.compile(r"chunk[0-9]+\Z")
CONTROLLED = {
    "output", "tmpdir", "download_dir", "temp_features", "temp_nodes",
    "temp_multipolygons", "reuse_featuredb", "only_download", "force",
    "overwrite", "append",
}


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(4 << 20), b""):
            h.update(block)
    return h.hexdigest()


def json_bytes(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def atomic_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("wb") as f:
        f.write(json_bytes(value))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    if os.name == "posix":
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def read_json(path: Path):
    with path.open("rb") as f:
        raw = f.read(MAX_JSON + 1)
    if len(raw) > MAX_JSON:
        raise ValueError(f"oversized JSON: {path.name}")
    return json.loads(raw)


@contextlib.contextmanager
def locked(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as f:
        if os.name == "nt":
            import msvcrt
            f.seek(0)
            if not f.read(1):
                f.write(b"\0")
                f.flush()
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def valid_run_id(value: str) -> str:
    if not RUN_ID.fullmatch(value):
        raise ValueError("BAKE_RUN_ID must be a nonempty safe build identifier")
    return value


def file_record(path: Path) -> dict:
    return {"size": path.stat().st_size, "sha256": digest(path)}


def validate_record(record: dict) -> None:
    if (not isinstance(record, dict)
            or not isinstance(record.get("size"), int)
            or record["size"] < 0
            or not re.fullmatch(r"[a-f0-9]{64}", record.get("sha256", ""))):
        raise ValueError("invalid checkpoint size or SHA-256")


class Store:
    def __init__(self, client=None):
        self.bucket = os.environ["BAKE_S3_BUCKET"]
        self.prefix = os.environ.get("BAKE_S3_PREFIX", "planetiler").strip("/")
        if not self.prefix or any(p in ("", ".", "..") for p in self.prefix.split("/")):
            raise ValueError("invalid BAKE_S3_PREFIX")
        if client is None:
            import boto3
            from botocore.config import Config
            client = boto3.client(
                "s3", endpoint_url=os.environ["BAKE_S3_ENDPOINT"],
                region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                config=Config(
                    signature_version="s3v4", s3={"addressing_style": "path"},
                    retries={"mode": "standard", "max_attempts": 8},
                    connect_timeout=10, read_timeout=90,
                ),
            )
        self.client = client

    def key(self, key: str) -> str:
        if any(p in ("", ".", "..") for p in key.split("/")) or "\\" in key:
            raise ValueError("invalid object key")
        return f"{self.prefix}/{key}"

    @staticmethod
    def missing(exc: Exception) -> bool:
        return getattr(exc, "response", {}).get("Error", {}).get("Code") in {
            "404", "NoSuchKey", "NotFound", "NoSuchUpload",
        }

    def get_json(self, key: str):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self.key(key))
        except Exception as exc:
            if self.missing(exc):
                return None
            raise
        with contextlib.closing(response["Body"]) as body:
            raw = body.read(MAX_JSON + 1)
        if len(raw) > MAX_JSON:
            raise ValueError("oversized S3 manifest")
        return json.loads(raw)

    def put_json(self, key: str, value) -> None:
        self.client.put_object(
            Bucket=self.bucket, Key=self.key(key), Body=json_bytes(value),
            ContentType="application/json",
        )

    def head(self, key: str):
        try:
            return self.client.head_object(Bucket=self.bucket, Key=self.key(key))
        except Exception as exc:
            if self.missing(exc):
                return None
            raise

    def upload(self, path: Path, key: str, state_dir: Path) -> dict:
        stat = path.stat()
        state_path = state_dir / (hashlib.sha256(key.encode()).hexdigest() + ".json")
        state = read_json(state_path) if state_path.exists() else {}
        identity = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
        if state.get("identity") == identity:
            record = state["record"]
        else:
            record = file_record(path)
            state = {}
        validate_record(record)
        head = self.head(key)
        if (head and head["ContentLength"] == record["size"]
                and head.get("Metadata", {}).get("sha256") == record["sha256"]):
            return record
        part_size = max(PART_SIZE, math.ceil(record["size"] / 10000 / (1 << 20)) * (1 << 20))
        parts = {}
        if state.get("upload_id"):
            try:
                marker = 0
                while True:
                    page = self.client.list_parts(
                        Bucket=self.bucket, Key=self.key(key),
                        UploadId=state["upload_id"], PartNumberMarker=marker,
                    )
                    parts.update({p["PartNumber"]: p for p in page.get("Parts", [])})
                    if not page.get("IsTruncated"):
                        break
                    marker = page["NextPartNumberMarker"]
            except Exception as exc:
                if not self.missing(exc):
                    raise
                state = {}
                parts = {}
        if not state.get("upload_id"):
            response = self.client.create_multipart_upload(
                Bucket=self.bucket, Key=self.key(key),
                Metadata={"sha256": record["sha256"]},
            )
            state = {
                "identity": identity, "record": record, "part_size": part_size,
                "upload_id": response["UploadId"],
            }
            atomic_json(state_path, state)
        part_size = state["part_size"]
        count = max(1, math.ceil(record["size"] / part_size))
        with path.open("rb") as f:
            for number in range(1, count + 1):
                expected = min(part_size, record["size"] - (number - 1) * part_size)
                if number in parts and parts[number]["Size"] == expected:
                    continue
                f.seek((number - 1) * part_size)
                data = f.read(expected)
                if len(data) != expected:
                    raise IOError("checkpoint file changed during upload")
                response = self.client.upload_part(
                    Bucket=self.bucket, Key=self.key(key), UploadId=state["upload_id"],
                    PartNumber=number, Body=data,
                    ContentMD5=base64.b64encode(hashlib.md5(data).digest()).decode(),
                )
                parts[number] = {"PartNumber": number, "ETag": response["ETag"], "Size": len(data)}
                if number % 16 == 0 or number == count:
                    LOG.info("upload %s: %d/%d parts", key, number, count)
        if path.stat().st_size != stat.st_size or path.stat().st_mtime_ns != stat.st_mtime_ns:
            raise IOError("checkpoint file changed during upload")
        self.client.complete_multipart_upload(
            Bucket=self.bucket, Key=self.key(key), UploadId=state["upload_id"],
            MultipartUpload={"Parts": [
                {"PartNumber": n, "ETag": parts[n]["ETag"]} for n in range(1, count + 1)
            ]},
        )
        head = self.head(key)
        if (not head or head["ContentLength"] != record["size"]
                or head.get("Metadata", {}).get("sha256") != record["sha256"]):
            raise IOError("completed S3 upload does not match checkpoint")
        state_path.unlink(missing_ok=True)
        return record

    def download(self, key: str, record: dict, path: Path) -> None:
        validate_record(record)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and file_record(path) == record:
            return
        head = self.head(key)
        if (not head or head["ContentLength"] != record["size"]
                or head.get("Metadata", {}).get("sha256") != record["sha256"]):
            raise IOError("S3 object does not match its committed manifest")
        part = path.with_name(path.name + ".part")
        start = part.stat().st_size if part.exists() else 0
        if start > record["size"]:
            part.unlink()
            start = 0
        if shutil.disk_usage(path.parent).free < record["size"] - start + (64 << 20):
            raise IOError("insufficient free space to resume download")
        if record["size"] == 0:
            part.touch()
        elif start < record["size"]:
            kwargs = {"Bucket": self.bucket, "Key": self.key(key), "IfMatch": head["ETag"]}
            if start:
                kwargs["Range"] = f"bytes={start}-"
            response = self.client.get_object(**kwargs)
            with contextlib.closing(response["Body"]) as body:
                if start and not response.get("ContentRange", "").startswith(f"bytes {start}-"):
                    raise IOError("S3 did not honor the checkpoint byte range")
                with part.open("ab" if start else "wb") as f:
                    for block in iter(lambda: body.read(1 << 20), b""):
                        if f.tell() + len(block) > record["size"]:
                            raise IOError("S3 response exceeds checkpoint size")
                        f.write(block)
                    f.flush()
                    os.fsync(f.fileno())
        if file_record(part) != record:
            part.unlink(missing_ok=True)
            raise IOError("downloaded checkpoint failed size/SHA-256 verification")
        os.replace(part, path)


def config_for(args: list[str]) -> dict:
    for arg in args:
        if not arg.startswith("--") or arg[2:].split("=")[0].replace("-", "_") in CONTROLLED:
            raise ValueError(f"unsupported or runner-controlled Planetiler argument: {arg}")
    return {"schema": 1, "profile_sha256": digest(JAR), "args": args}


def feature_files(feature_dir: Path) -> list[Path]:
    strings = feature_dir / "feature.db.strings"
    manifest = feature_dir / "feature.db.manifest"
    read_json(strings)
    data = read_json(manifest)
    names = [entry["filename"] for entry in data["chunks"]]
    if len(set(names)) != len(names) or any(not CHUNK.fullmatch(n) for n in names):
        raise ValueError("invalid sorted chunk manifest")
    files = [strings, manifest] + [feature_dir / n for n in names]
    if any(not p.is_file() or p.is_symlink() for p in files):
        raise ValueError("incomplete sorted feature checkpoint")
    return files


def valid_pmtiles(path: Path, args: list[str]) -> None:
    with path.open("rb") as f:
        header = f.read(127)
    if len(header) != 127 or header[:8] != b"PMTiles\x03" or header[99] != 1:
        raise ValueError("bake did not produce PMTiles v3 MVT")
    values = dict(a[2:].split("=", 1) for a in args if "=" in a)
    if header[100] != int(values.get("minzoom", 0)) or header[101] != int(values.get("maxzoom", 14)):
        raise ValueError("baked zoom coverage does not match the requested build")
    size = path.stat().st_size
    for offset in (8, 24, 40, 56):
        pos, length = struct.unpack_from("<QQ", header, offset)
        if pos > size or length > size - pos:
            raise ValueError("truncated PMTiles archive")


def java(args: list[str]) -> list[str]:
    return ["java", "-jar", str(JAR), *args]


def publish_ready(store: Store, manifest: dict) -> None:
    current = store.get_json("ready.json")
    if not current or current["created_at"] <= manifest["created_at"]:
        store.put_json("ready.json", manifest)


def build(store: Store, args: list[str]) -> None:
    run_id = valid_run_id(os.environ["BAKE_RUN_ID"])
    root = Path(os.environ.get("BAKE_WORKDIR", "/scratch"))
    run = root / "runs" / run_id
    run.mkdir(parents=True, exist_ok=True)
    config = config_for(args)
    prefix = f"runs/{run_id}"
    with locked(root / "bake.lock"):
        inputs = store.get_json(f"{prefix}/inputs.json")
        if inputs and inputs["config"] != config:
            raise ValueError("build ID already belongs to different inputs/profile/options; use a new Job name")
        complete = store.get_json(f"{prefix}/artifact.json")
        if complete:
            if not inputs or complete["fingerprint"] != inputs["fingerprint"]:
                raise ValueError("completed build fingerprint mismatch")
            validate_record(complete["artifact"])
            head = store.head(f"{prefix}/planet.pmtiles")
            if (head and head["ContentLength"] == complete["artifact"]["size"]
                    and head.get("Metadata", {}).get("sha256") == complete["artifact"]["sha256"]):
                LOG.info("build %s already published; not running Planetiler", run_id)
                publish_ready(store, complete)
                return
            LOG.warning("published artifact unavailable; recovering from retained local/feature checkpoints")
        local_inputs = run / "inputs.json"
        if inputs:
            atomic_json(local_inputs, inputs)
        elif local_inputs.exists():
            inputs = read_json(local_inputs)
            if inputs["config"] != config:
                raise ValueError("local build configuration changed; use a new Job name")
            store.put_json(f"{prefix}/inputs.json", inputs)
        sources = root / "sources"
        temp = run / "tmp"
        output = run / "planet.pmtiles"
        checkpoint = store.get_json(f"{prefix}/features.json")
        feature_dir = temp / "feature.db"
        baked = run / "baked.json"
        if checkpoint and not baked.exists():
            if not inputs or checkpoint["fingerprint"] != inputs["fingerprint"]:
                raise ValueError("feature checkpoint fingerprint mismatch")
            for name, record in checkpoint["files"].items():
                if name not in ("feature.db.strings", "feature.db.manifest") and not CHUNK.fullmatch(name):
                    raise ValueError("invalid feature checkpoint filename")
                store.download(f"{prefix}/features/{name}", record, feature_dir / name)
            feature_files(feature_dir)
            LOG.info("restored completed sorted features for %s", run_id)
        new_inputs = inputs is None
        if new_inputs:
            subprocess.run(java([
                *args, "--only_download=true", "--force",
                f"--download-dir={sources}", f"--tmpdir={run / 'download-tmp'}",
                f"--output={run / 'download.pmtiles'}",
            ]), check=True)
            files = {}
            for path in sorted(sources.rglob("*")):
                if path.is_file() and not path.is_symlink():
                    files[str(path.relative_to(sources)).replace("\\", "/")] = file_record(path)
            inputs = {
                "config": config, "sources": files,
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            inputs["fingerprint"] = hashlib.sha256(json_bytes(inputs)).hexdigest()
            atomic_json(local_inputs, inputs)
            store.put_json(f"{prefix}/inputs.json", inputs)
        if baked.exists():
            if read_json(baked)["fingerprint"] != inputs["fingerprint"]:
                raise ValueError("completed local archive fingerprint mismatch")
            valid_pmtiles(output, args)
            LOG.info("completed local archive found; resuming publication only")
        else:
            reusable = False
            try:
                feature_files(feature_dir)
                reusable = True
            except (OSError, ValueError, KeyError, TypeError):
                # Planetiler's existence-only check would accept torn metadata.
                (feature_dir / "feature.db.manifest").unlink(missing_ok=True)
            if not reusable and not new_inputs:
                for name, expected in inputs["sources"].items():
                    path = sources / name
                    if not path.is_file() or file_record(path) != expected:
                        raise ValueError("frozen source inputs changed or disappeared; use a new Job name")
            effective = args
            if reusable:
                effective = [
                    a for a in args
                    if a[2:].split("=")[0].replace("-", "_") not in
                    {"download", "fetch_wikidata", "use_wikidata"}
                ] + ["--download=false", "--fetch-wikidata=false", "--use-wikidata=false"]
            command = java([
                *effective, "--reuse_featuredb=true", "--force",
                f"--download-dir={sources}", f"--tmpdir={temp}", f"--output={output}",
            ])
            LOG.info("starting %s (%s)", run_id, "archive from checkpoint" if reusable else "source processing")
            process = subprocess.Popen(command)
            errors = []
            finished = threading.Event()

            def save_features():
                try:
                    while not finished.is_set():
                        try:
                            files = feature_files(feature_dir)
                            break
                        except (OSError, ValueError, KeyError, TypeError):
                            finished.wait(5)
                    else:
                        files = feature_files(feature_dir)
                    if checkpoint:
                        return
                    LOG.info("sorted features ready; checkpointing to S3")
                    records = {
                        path.name: store.upload(path, f"{prefix}/features/{path.name}", run / "uploads")
                        for path in files
                    }
                    store.put_json(f"{prefix}/features.json", {
                        "fingerprint": inputs["fingerprint"], "files": records,
                    })
                    LOG.info("sorted feature checkpoint committed")
                except Exception as exc:
                    errors.append(exc)
                    LOG.exception("feature checkpoint publication interrupted")

            thread = threading.Thread(target=save_features, daemon=True)
            thread.start()
            try:
                if process.wait() != 0:
                    raise RuntimeError("Planetiler failed; retained checkpoints will be reused on retry")
                valid_pmtiles(output, args)
                atomic_json(baked, {"fingerprint": inputs["fingerprint"]})
            finally:
                finished.set()
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                thread.join()
            if errors:
                raise RuntimeError("feature checkpoint upload failed; retry will not rebake completed output") from errors[0]
        if not store.get_json(f"{prefix}/features.json"):
            records = {
                path.name: store.upload(path, f"{prefix}/features/{path.name}", run / "uploads")
                for path in feature_files(feature_dir)
            }
            store.put_json(f"{prefix}/features.json", {
                "fingerprint": inputs["fingerprint"], "files": records,
            })
        record = store.upload(output, f"{prefix}/planet.pmtiles", run / "uploads")
        manifest = {
            "schema": 1, "run_id": run_id, "fingerprint": inputs["fingerprint"],
            "created_at": inputs["created_at"], "artifact": record,
        }
        store.put_json(f"{prefix}/artifact.json", manifest)
        publish_ready(store, manifest)
        LOG.info("build %s published; the independent import job will activate it", run_id)


def import_ready(store: Store) -> None:
    manifest = store.get_json("ready.json")
    if manifest is None:
        LOG.info("no completed bake published yet")
        return
    run_id = valid_run_id(manifest["run_id"])
    if manifest.get("schema") != 1 or store.get_json(f"runs/{run_id}/artifact.json") != manifest:
        raise ValueError("ready pointer does not match a committed artifact")
    record = manifest["artifact"]
    validate_record(record)
    data = Path(os.environ["TILE_DATA_DIR"])
    work = data / "staging" / f"bake-{run_id}"
    marker = work / "installed.json"
    with locked(data / "bake-import.lock"):
        if marker.exists() and read_json(marker) == manifest:
            active = data / "active.json"
            release = data / "releases" / run_id / "planet.pmtiles"
            if (active.exists() and read_json(active).get("version") == run_id
                    and release.exists() and release.stat().st_size == record["size"]):
                LOG.info("build %s is already active", run_id)
                return
        existing = data / "releases" / run_id / "planet.pmtiles"
        if existing.exists() and file_record(existing) == record:
            output = existing
        else:
            output = work / "planet.pmtiles"
            store.download(f"runs/{run_id}/planet.pmtiles", record, output)
        subprocess.run([
            "/usr/local/bin/streetcryptid-map-server", "tiles", "import", str(output), run_id,
        ], check=True)
        atomic_json(marker, manifest)
        LOG.info("build %s imported and deployment notified", run_id)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = sys.argv[1:]
    if not args or args[0] not in ("build", "import", "check"):
        os.execvp("java", java(args))
    def terminate(*_):
        raise InterruptedError("terminated; checkpoints retained")

    signal.signal(signal.SIGTERM, terminate)
    store = Store()
    if args[0] == "build":
        build(store, args[1:])
    elif args[0] == "import":
        import_ready(store)
    else:
        key = f"checks/{uuid.uuid4()}"
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / "source"
                source.write_bytes(b"checkpoint-transfer-probe" * 10000)
                record = store.upload(source, key, root / "uploads")
                output = root / "output"
                output.with_name("output.part").write_bytes(source.read_bytes()[:127])
                store.download(key, record, output)
                if output.read_bytes() != source.read_bytes():
                    raise IOError("S3 transfer check failed")
        finally:
            store.client.delete_object(Bucket=store.bucket, Key=store.key(key))
        LOG.info("S3 multipart upload and byte-range resume check passed")


if __name__ == "__main__":
    main()
