import hashlib
import io
import json
import os
from pathlib import Path
import struct
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import bake


class Missing(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.uploads = {}
        self.uploaded_parts = []
        self.requests = []
        self.fail_part = None
        self.fail_read = False

    def head_object(self, *, Key, **_):
        if Key not in self.objects:
            raise Missing()
        data, metadata = self.objects[Key]
        return {
            "ContentLength": len(data), "Metadata": metadata,
            "ETag": hashlib.md5(data).hexdigest(),
        }

    def get_object(self, *, Key, Range=None, IfMatch=None, **_):
        self.requests.append((Key, Range))
        head = self.head_object(Key=Key)
        if IfMatch and IfMatch != head["ETag"]:
            raise RuntimeError("object changed")
        data = self.objects[Key][0]
        start = int(Range[6:-1]) if Range else 0
        body = io.BytesIO(data[start:])
        if self.fail_read:
            self.fail_read = False

            class InterruptedBody(io.BytesIO):
                def read(self, size=-1):
                    if self.tell():
                        raise ConnectionError("interrupted download")
                    return super().read(min(size, 17))

            body = InterruptedBody(data[start:])
        return {
            "Body": body, "ContentRange": f"bytes {start}-{len(data) - 1}/{len(data)}",
        }

    def put_object(self, *, Key, Body, **_):
        self.objects[Key] = (Body, {})

    def create_multipart_upload(self, *, Key, Metadata, **_):
        upload_id = str(len(self.uploads) + 1)
        self.uploads[upload_id] = {"key": Key, "metadata": Metadata, "parts": {}}
        return {"UploadId": upload_id}

    def list_parts(self, *, UploadId, **_):
        return {"Parts": [
            {"PartNumber": n, "Size": len(data), "ETag": hashlib.md5(data).hexdigest()}
            for n, data in self.uploads[UploadId]["parts"].items()
        ]}

    def upload_part(self, *, UploadId, PartNumber, Body, **_):
        if PartNumber == self.fail_part:
            self.fail_part = None
            raise ConnectionError("interrupted upload")
        self.uploaded_parts.append(PartNumber)
        self.uploads[UploadId]["parts"][PartNumber] = Body
        return {"ETag": hashlib.md5(Body).hexdigest()}

    def complete_multipart_upload(self, *, UploadId, MultipartUpload, **_):
        upload = self.uploads[UploadId]
        data = b"".join(upload["parts"][p["PartNumber"]] for p in MultipartUpload["Parts"])
        self.objects[upload["key"]] = (data, upload["metadata"])


def pmtiles() -> bytes:
    data = bytearray(127)
    data[:8] = b"PMTiles\x03"
    data[99:102] = bytes([1, 0, 14])
    return bytes(data)


class BakeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.env = patch.dict(os.environ, {
            "BAKE_S3_BUCKET": "scratch-map-bakes", "BAKE_S3_PREFIX": "tests",
            "BAKE_WORKDIR": str(self.root), "BAKE_RUN_ID": "build-1",
            "TILE_DATA_DIR": str(self.root / "tiles"),
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        self.client = FakeS3()
        self.store = bake.Store(self.client)

    def object(self, key, data):
        record = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
        self.client.objects[self.store.key(key)] = (data, {"sha256": record["sha256"]})
        return record

    def manifest(self):
        record = self.object("runs/build-1/planet.pmtiles", pmtiles())
        manifest = {
            "schema": 1, "run_id": "build-1", "fingerprint": "fingerprint",
            "created_at": "2026-10-30T00:00:00Z", "artifact": record,
        }
        self.store.put_json("ready.json", manifest)
        self.store.put_json("runs/build-1/artifact.json", manifest)
        return manifest

    def test_interrupted_upload_reuses_server_parts_after_process_restart(self):
        path = self.root / "blob"
        path.write_bytes(b"a" * (1 << 20) + b"b" * (1 << 20) + b"last")
        self.client.fail_part = 2
        with patch.object(bake, "PART_SIZE", 1 << 20):
            with self.assertRaises(ConnectionError):
                self.store.upload(path, "blob", self.root / "uploads")
            restarted = bake.Store(self.client)
            record = restarted.upload(path, "blob", self.root / "uploads")
        self.assertEqual(self.client.uploaded_parts, [1, 2, 3])
        self.assertEqual(len(self.client.uploads), 1)
        self.assertEqual(record, bake.file_record(path))
        self.assertEqual(self.client.objects[self.store.key("blob")][0], path.read_bytes())

    def test_interrupted_download_resumes_verified_object_at_saved_offset(self):
        data = b"terrain-free-vector-data" * 100
        record = self.object("artifact", data)
        output = self.root / "artifact"
        self.client.fail_read = True
        with self.assertRaises(ConnectionError):
            self.store.download("artifact", record, output)
        self.assertEqual(output.with_name("artifact.part").stat().st_size, 17)
        bake.Store(self.client).download("artifact", record, output)
        self.assertEqual(self.client.requests[-1][1], "bytes=17-")
        self.assertEqual(output.read_bytes(), data)

    def test_corrupt_partial_never_becomes_a_completed_artifact(self):
        record = self.object("artifact", b"right contents")
        output = self.root / "artifact"
        output.with_name("artifact.part").write_bytes(b"wrong")
        with self.assertRaisesRegex(IOError, "SHA-256"):
            self.store.download("artifact", record, output)
        self.assertFalse(output.exists())
        self.assertFalse(output.with_name("artifact.part").exists())

    def test_completed_remote_build_never_launches_java(self):
        manifest = self.manifest()
        self.store.put_json("runs/build-1/inputs.json", {
            "config": {"profile": "same"}, "fingerprint": manifest["fingerprint"],
        })
        with patch.object(bake, "config_for", return_value={"profile": "same"}), \
                patch.object(bake.subprocess, "run") as run, \
                patch.object(bake.subprocess, "Popen") as popen:
            bake.build(self.store, [])
        run.assert_not_called()
        popen.assert_not_called()

    def test_changed_profile_cannot_reuse_a_build_id(self):
        self.store.put_json("runs/build-1/inputs.json", {"config": {"profile": "old"}})
        with patch.object(bake, "config_for", return_value={"profile": "new"}), \
                patch.object(bake.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(ValueError, "different inputs"):
                bake.build(self.store, [])
        popen.assert_not_called()

    def test_completed_local_bake_retries_upload_without_rebaking(self):
        inputs = {
            "config": {"profile": "same"}, "fingerprint": "fingerprint",
            "created_at": "2026-10-30T00:00:00Z",
        }
        self.store.put_json("runs/build-1/inputs.json", inputs)
        self.store.put_json("runs/build-1/features.json", {
            "fingerprint": "fingerprint", "files": {},
        })
        run_dir = self.root / "runs" / "build-1"
        bake.atomic_json(run_dir / "baked.json", {"fingerprint": "fingerprint"})
        (run_dir / "planet.pmtiles").write_bytes(pmtiles())
        with patch.object(bake, "config_for", return_value=inputs["config"]), \
                patch.object(bake, "feature_files", return_value=[]), \
                patch.object(bake.subprocess, "run") as run, \
                patch.object(bake.subprocess, "Popen") as popen:
            bake.build(self.store, [])
        run.assert_not_called()
        popen.assert_not_called()
        self.assertEqual(self.store.get_json("ready.json")["artifact"]["size"], 127)

    def test_checkpoint_rejects_torn_metadata_and_path_traversal(self):
        folder = self.root / "feature.db"
        folder.mkdir()
        bake.atomic_json(folder / "feature.db.strings", {})
        (folder / "feature.db.manifest").write_text('{"chunks":')
        with self.assertRaises(ValueError):
            bake.feature_files(folder)
        bake.atomic_json(folder / "feature.db.manifest", {"chunks": [{"filename": "../outside"}]})
        with self.assertRaisesRegex(ValueError, "invalid sorted"):
            bake.feature_files(folder)

    def test_feature_checkpoint_restores_without_original_source_files(self):
        inputs = {
            "config": {"profile": "same"}, "fingerprint": "fingerprint",
            "created_at": "2026-10-30T00:00:00Z", "sources": {"missing.osm.pbf": {}},
        }
        self.store.put_json("runs/build-1/inputs.json", inputs)
        files = {
            "feature.db.strings": b'{"layerStrings":[],"valueStrings":[]}',
            "feature.db.manifest": b'{"chunks":[{"filename":"chunk1"}],"totalFeatures":1}',
            "chunk1": b"sorted features",
        }
        records = {
            name: self.object(f"runs/build-1/features/{name}", data)
            for name, data in files.items()
        }
        self.store.put_json("runs/build-1/features.json", {
            "fingerprint": "fingerprint", "files": records,
        })

        def finish_archive(command):
            output = next(a.split("=", 1)[1] for a in command if a.startswith("--output="))
            Path(output).write_bytes(pmtiles())
            return SimpleNamespace(wait=lambda: 0, poll=lambda: 0)

        with patch.object(bake, "config_for", return_value=inputs["config"]), \
                patch.object(bake.subprocess, "run") as download, \
                patch.object(bake.subprocess, "Popen", side_effect=finish_archive) as process:
            bake.build(self.store, ["--download", "--fetch-wikidata"])
        download.assert_not_called()
        self.assertIn("--download=false", process.call_args.args[0])
        self.assertIn("--fetch-wikidata=false", process.call_args.args[0])
        self.assertEqual(self.store.get_json("ready.json")["artifact"]["size"], 127)

    def test_failed_import_reuses_download_and_does_not_mark_success(self):
        manifest = self.manifest()
        with patch.object(bake.subprocess, "run", side_effect=RuntimeError("notify failed")):
            with self.assertRaisesRegex(RuntimeError, "notify failed"):
                bake.import_ready(self.store)
        work = Path(os.environ["TILE_DATA_DIR"]) / "staging" / "bake-build-1"
        self.assertFalse((work / "installed.json").exists())
        self.assertEqual((work / "planet.pmtiles").read_bytes(), pmtiles())
        requests = len(self.client.requests)
        with patch.object(bake.subprocess, "run") as run:
            bake.import_ready(self.store)
        run.assert_called_once()
        artifact_requests = [r for r in self.client.requests[requests:] if r[0].endswith(".pmtiles")]
        self.assertEqual(artifact_requests, [])
        self.assertEqual(bake.read_json(work / "installed.json"), manifest)

    def test_import_retry_recovers_artifact_moved_before_manifest_was_saved(self):
        self.manifest()
        existing = Path(os.environ["TILE_DATA_DIR"]) / "releases" / "build-1" / "planet.pmtiles"
        existing.parent.mkdir(parents=True)
        existing.write_bytes(pmtiles())
        with patch.object(bake.subprocess, "run") as run:
            bake.import_ready(self.store)
        self.assertEqual(run.call_args.args[0][-2], str(existing))
        self.assertFalse(any(key.endswith(".pmtiles") for key, _ in self.client.requests))

    def test_import_requires_committed_manifest(self):
        manifest = self.manifest()
        manifest["artifact"]["sha256"] = "0" * 64
        self.store.put_json("ready.json", manifest)
        with patch.object(bake.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "committed artifact"):
                bake.import_ready(self.store)
        run.assert_not_called()

    def test_completed_import_poll_does_not_reread_the_whole_archive(self):
        manifest = self.manifest()
        data = Path(os.environ["TILE_DATA_DIR"])
        bake.atomic_json(data / "active.json", {"version": "build-1"})
        bake.atomic_json(data / "staging" / "bake-build-1" / "installed.json", manifest)
        release = data / "releases" / "build-1" / "planet.pmtiles"
        release.parent.mkdir(parents=True)
        release.write_bytes(pmtiles())
        with patch.object(bake, "file_record", side_effect=AssertionError("unexpected archive reread")), \
                patch.object(bake.subprocess, "run") as run:
            bake.import_ready(self.store)
        run.assert_not_called()

    def test_ready_pointer_cannot_regress_to_an_older_build(self):
        manifest = self.manifest()
        older = dict(manifest, created_at="2026-10-01T00:00:00Z")
        bake.publish_ready(self.store, older)
        self.assertEqual(self.store.get_json("ready.json"), manifest)

    def test_unfinalized_pmtiles_is_not_published(self):
        path = self.root / "bad.pmtiles"
        data = bytearray(pmtiles())
        struct.pack_into("<QQ", data, 56, 127, 100)
        path.write_bytes(data)
        with self.assertRaisesRegex(ValueError, "truncated"):
            bake.valid_pmtiles(path, [])


if __name__ == "__main__":
    unittest.main()
