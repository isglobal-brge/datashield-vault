import datetime as dt
import hashlib
import importlib.util
import io
from pathlib import Path
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import yaml


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "store_controller", ROOT / "controller" / "controller.py"
)
controller = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(controller)


class FakeBody(io.BytesIO):
    pass


class FakePaginator:
    def __init__(self, objects):
        self.objects = objects

    def paginate(self, Bucket, Prefix, Delimiter=None):
        contents = []
        common_prefixes = set()
        for key, value in sorted(self.objects.items()):
            if not key.startswith(Prefix):
                continue
            if Delimiter:
                rest = key[len(Prefix):]
                if Delimiter in rest:
                    common_prefixes.add(Prefix + rest.split(Delimiter, 1)[0] + Delimiter)
                    continue
            contents.append({
                "Key": key,
                "Size": len(value),
                "LastModified": dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc),
                "ETag": f'"{hashlib.md5(value).hexdigest()}"',
            })
        page = {}
        if contents:
            page["Contents"] = contents
        if common_prefixes:
            page["CommonPrefixes"] = [
                {"Prefix": prefix} for prefix in sorted(common_prefixes)
            ]
        yield page


class FakeS3:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.on_upload = None
        self.fail_upload_suffix = None
        self.upload_history = []
        self.get_requests = []

    def get_paginator(self, name):
        if name != "list_objects_v2":
            raise ValueError(name)
        return FakePaginator(self.objects)

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        digest = hashlib.md5(self.objects[Key]).hexdigest()
        return {
            "ContentLength": len(self.objects[Key]),
            "LastModified": dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc),
            "ETag": f'"{digest}"',
            "VersionId": f"version-{digest}",
        }

    def get_object(self, Bucket, Key, VersionId=None):
        if Key not in self.objects:
            raise KeyError(Key)
        expected_version = f"version-{hashlib.md5(self.objects[Key]).hexdigest()}"
        if VersionId is not None and VersionId != expected_version:
            raise KeyError((Key, VersionId))
        self.get_requests.append((Key, VersionId))
        return {
            "Body": FakeBody(self.objects[Key]),
            "ETag": f'"{hashlib.md5(self.objects[Key]).hexdigest()}"',
        }

    def upload_file(self, Filename, Bucket, Key):
        if self.fail_upload_suffix and Key.endswith(self.fail_upload_suffix):
            raise RuntimeError("injected upload failure")
        self.objects[Key] = Path(Filename).read_bytes()
        self.upload_history.append(Key)
        if self.on_upload:
            self.on_upload(Key)

    def put_object(self, Bucket, Key, Body, **kwargs):
        if kwargs.get("IfNoneMatch") == "*" and Key in self.objects:
            raise RuntimeError("precondition failed")
        value = Body.read() if hasattr(Body, "read") else bytes(Body)
        self.objects[Key] = value
        return {"ETag": f'"{hashlib.md5(value).hexdigest()}"'}

    def delete_object(self, Bucket, Key, IfMatch=None):
        if Key not in self.objects:
            raise KeyError(Key)
        etag = f'"{hashlib.md5(self.objects[Key]).hexdigest()}"'
        if IfMatch is not None and IfMatch != etag:
            raise RuntimeError("precondition failed")
        del self.objects[Key]
        return {}

    def delete_objects(self, Bucket, Delete):
        for item in Delete.get("Objects", []):
            self.objects.pop(item["Key"], None)
        return {}


class ReconcileTests(unittest.TestCase):
    def test_success_telemetry_does_not_retain_exact_collection_counts(self):
        controller.last_reconcile.clear()

        controller.record_success("study_ct_v1", 6, 2)

        self.assertEqual(
            set(controller.last_reconcile["study_ct_v1"]), {"at"})

    def test_reconcile_rejects_invalid_dataset_id_before_storage_access(self):
        with patch.object(controller, "get_s3") as get_s3:
            with self.assertRaisesRegex(ValueError, "invalid dataset id"):
                controller.reconcile_dataset("../other")
        get_s3.assert_not_called()

    def setUp(self):
        self.bucket = "imaging-data"
        self.old_bucket = controller.BUCKET
        self.old_get_s3 = controller.get_s3
        controller.BUCKET = self.bucket

    def tearDown(self):
        controller.BUCKET = self.old_bucket
        controller.get_s3 = self.old_get_s3

    def test_reconcile_removes_managed_artifacts_when_images_disappear(self):
        prefix = "datasets/study_ct_v1"
        objects = self._published_objects(prefix, [("case001", "patient-a")])
        stale_artifacts = [
            "indexes/content_hash_index.parquet",
            "indexes/masks_content_hash_index.parquet",
            "metadata/sample_manifests.parquet",
        ]
        objects.update({
            f"{prefix}/{suffix}": b"stale" for suffix in stale_artifacts
        })
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        n_samples, n_masks = controller.reconcile_dataset("study_ct_v1")

        self.assertEqual((n_samples, n_masks), (0, 0))
        for suffix in controller.MANAGED_ARTIFACTS:
            self.assertNotIn(f"{prefix}/{suffix}", s3.objects)

    def test_reconcile_does_not_degrade_corrupt_dataset_without_images(self):
        prefix = "datasets/study_ct_v1"
        objects = self._published_objects(prefix, [("case001", "patient-a")])
        objects[f"{prefix}/manifest.yaml"] = b"not: [valid"
        before = dict(objects)
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        with self.assertRaisesRegex(ValueError, "manifest is corrupt"):
            controller.reconcile_dataset("study_ct_v1")

        self.assertEqual(s3.objects, before)

    def test_reconcile_rejects_manifest_pointing_to_another_collection(self):
        prefix = "datasets/study_ct_v1"
        objects = self._published_objects(prefix, [("case001", "patient-a")])
        manifest = yaml.safe_load(objects[f"{prefix}/manifest.yaml"])
        manifest["assets"]["images"]["uri"] = (
            "s3://imaging-data/datasets/other/source/images/")
        objects[f"{prefix}/manifest.yaml"] = yaml.safe_dump(
            manifest, sort_keys=False).encode("utf-8")
        objects[f"{prefix}/source/images/case001.nii.gz"] = b"image"
        before = dict(objects)
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        with self.assertRaisesRegex(ValueError, "canonical collection root"):
            controller.reconcile_dataset("study_ct_v1")

        self.assertEqual(s3.objects, before)

    def test_reconcile_preserves_an_active_foreign_lock(self):
        prefix = "datasets/study_ct_v1"
        lock_key = f"{prefix}/{controller.PUBLISH_LOCK}"
        objects = self._published_objects(prefix, [("case001", "patient-a")])
        objects[lock_key] = b'{"status":"publishing","owner":"other"}'
        before = dict(objects)
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        with self.assertRaises(controller.PublishInProgress):
            controller.reconcile_dataset("study_ct_v1")

        self.assertEqual(s3.objects, before)

    def test_reconcile_never_removes_a_replacement_lock(self):
        prefix = "datasets/study_ct_v1"
        lock_key = f"{prefix}/{controller.PUBLISH_LOCK}"
        objects = self._published_objects(prefix, [("case001", "patient-a")])
        objects[f"{prefix}/source/images/case001.nii.gz"] = b"image"
        original_manifest = objects[f"{prefix}/manifest.yaml"]
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        def replace_lock(_):
            s3.objects[lock_key] = (
                b'{"status":"publishing","owner":"replacement"}')
            s3.on_upload = None

        s3.on_upload = replace_lock
        with self.assertRaisesRegex(
                controller.LockOwnershipLost, "ownership was lost"):
            controller.reconcile_dataset("study_ct_v1")
        self.assertEqual(
            s3.objects[lock_key],
            b'{"status":"publishing","owner":"replacement"}',
        )
        self.assertEqual(s3.objects[f"{prefix}/manifest.yaml"], original_manifest)

    def test_empty_source_cleanup_stops_after_lock_ownership_changes(self):
        prefix = "datasets/study_ct_v1"
        lock_key = f"{prefix}/{controller.PUBLISH_LOCK}"
        objects = self._published_objects(prefix, [("case001", "patient-a")])
        before_artifacts = dict(objects)
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3
        read_metadata = controller.read_existing_samples_metadata

        def replace_lock(*args):
            table = read_metadata(*args)
            s3.objects[lock_key] = (
                b'{"status":"publishing","owner":"replacement"}')
            return table

        with patch.object(
                controller, "read_existing_samples_metadata",
                side_effect=replace_lock), self.assertRaisesRegex(
                    controller.LockOwnershipLost, "ownership was lost"):
            controller.reconcile_dataset("study_ct_v1")

        for key, value in before_artifacts.items():
            self.assertEqual(s3.objects[key], value)
        self.assertEqual(
            s3.objects[lock_key],
            b'{"status":"publishing","owner":"replacement"}',
        )

    def test_source_roster_change_rolls_back_managed_artifacts(self):
        prefix = "datasets/study_ct_v1"
        objects = self._published_objects(prefix, [("case001", "patient-a")])
        objects[f"{prefix}/source/images/case001.nii.gz"] = b"image"
        before = {
            key: value for key, value in objects.items()
            if key.rsplit(f"{prefix}/", 1)[-1] in controller.MANAGED_ARTIFACTS
        }
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        def add_source(_):
            s3.objects[f"{prefix}/source/images/case002.nii.gz"] = b"new"
            s3.on_upload = None

        s3.on_upload = add_source
        with self.assertRaisesRegex(RuntimeError, "source roster changed"):
            controller.reconcile_dataset("study_ct_v1")

        after = {
            key: value for key, value in s3.objects.items()
            if key.rsplit(f"{prefix}/", 1)[-1] in controller.MANAGED_ARTIFACTS
        }
        self.assertEqual(after, before)
        self.assertNotIn(f"{prefix}/{controller.PUBLISH_LOCK}", s3.objects)

    def test_artifact_upload_failure_restores_exact_previous_set(self):
        prefix = "datasets/study_ct_v1"
        objects = self._published_objects(prefix, [("case001", "patient-a")])
        objects[f"{prefix}/source/images/case001.nii.gz"] = b"image"
        objects[f"{prefix}/indexes/content_hash_index.parquet"] = b"old-index"
        before = dict(objects)
        s3 = FakeS3(objects)
        s3.fail_upload_suffix = "metadata/sample_manifests.parquet"
        controller.get_s3 = lambda: s3

        with self.assertRaisesRegex(RuntimeError, "injected upload failure"):
            controller.reconcile_dataset("study_ct_v1")

        self.assertEqual(s3.objects, before)

    def test_reconcile_removes_stale_mask_index_when_masks_disappear(self):
        prefix = "datasets/study_ct_v1"
        stale_mask_index = f"{prefix}/indexes/masks_content_hash_index.parquet"
        published = self._published_objects(prefix, [("case001", "patient-a")])
        s3 = FakeS3(published | {
            f"{prefix}/source/images/case001.nii.gz": b"image",
            stale_mask_index: b"stale",
        })
        controller.get_s3 = lambda: s3

        n_samples, n_masks = controller.reconcile_dataset("study_ct_v1")

        self.assertEqual((n_samples, n_masks), (1, 0))
        self.assertIn(f"{prefix}/manifest.yaml", s3.objects)
        self.assertIn(f"{prefix}/indexes/content_hash_index.parquet", s3.objects)
        self.assertNotIn(stale_mask_index, s3.objects)

    def test_reconcile_records_current_source_version_ids(self):
        prefix = "datasets/study_ct_v1"
        image_key = f"{prefix}/source/images/case001.nii.gz"
        objects = self._published_objects(
            prefix, [("case001", "patient-a")]
        )
        objects[image_key] = b"image"
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        controller.reconcile_dataset("study_ct_v1")

        index = pq.read_table(pa.BufferReader(
            s3.objects[f"{prefix}/indexes/content_hash_index.parquet"]
        ))
        expected = f"version-{hashlib.md5(b'image').hexdigest()}"
        self.assertEqual(index["version_id"].to_pylist(), [expected])
        self.assertIn((image_key, expected), s3.get_requests)

    def test_reconcile_preserves_contract_and_repeated_patient_per_dataset(self):
        prefix_a = "datasets/study_a"
        prefix_b = "datasets/study_b"
        objects = self._published_objects(
            prefix_a,
            [("case001", "patient-a"), ("case002", "patient-a")],
            title="curated title",
            labels=["case", "case"],
            label_levels=["case", "control"],
        )
        objects.update(self._published_objects(
            prefix_b, [("case101", "patient-b")], title="other dataset"
        ))
        objects.update({
            f"{prefix_a}/source/images/site-a/case001.nii.gz": b"one",
            f"{prefix_a}/source/images/site-b/case002.nii.gz": b"two",
            f"{prefix_b}/source/images/case101.nii.gz": b"other",
        })
        untouched_b = dict(objects)
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        self.assertEqual(controller.reconcile_dataset("study_a"), (2, 0))
        self.assertEqual(
            s3.upload_history[-1], f"{prefix_a}/manifest.yaml")

        manifest = yaml.safe_load(s3.objects[f"{prefix_a}/manifest.yaml"])
        self.assertEqual(manifest["title"], "curated title")
        self.assertEqual(manifest["metadata"]["privacy_unit_col"], "patient_id")
        self.assertEqual(
            manifest["metadata"]["privacy_unit_canonicalization"],
            "trim-utf8-v2",
        )
        self.assertEqual(
            manifest["metadata"]["label_levels"], ["case", "control"])
        table = pq.read_table(pa.BufferReader(
            s3.objects[f"{prefix_a}/metadata/samples.parquet"]
        ))
        self.assertEqual(table["patient_id"].to_pylist(), ["patient-a", "patient-a"])
        self.assertEqual(table["diagnosis"].to_pylist(), ["case", "case"])
        for key, value in untouched_b.items():
            if key.startswith(f"{prefix_b}/"):
                self.assertEqual(s3.objects[key], value)
        self.assertNotIn(f"{prefix_a}/{controller.PUBLISH_LOCK}", s3.objects)

    def test_inventory_ignores_noncanonical_dataset_prefixes(self):
        objects = {
            "datasets/study_a/manifest.yaml": b"valid",
            "datasets/Study_B/manifest.yaml": b"noncanonical",
        }
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        self.assertEqual(
            [item["dataset_id"] for item in controller.list_datasets()],
            ["study_a"],
        )

    def test_reconcile_rejects_duplicate_sample_ids_without_replacing_manifest(self):
        prefix = "datasets/study_ct_v1"
        objects = self._published_objects(prefix, [("case001", "patient-a")])
        original_manifest = objects[f"{prefix}/manifest.yaml"]
        objects.update({
            f"{prefix}/source/images/site-a/case001.nii.gz": b"one",
            f"{prefix}/source/images/site-b/case001.nii.gz": b"two",
        })
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        with self.assertRaisesRegex(ValueError, "duplicate sample_id"):
            controller.reconcile_dataset("study_ct_v1")

        self.assertEqual(s3.objects[f"{prefix}/manifest.yaml"], original_manifest)
        self.assertNotIn(f"{prefix}/{controller.PUBLISH_LOCK}", s3.objects)

    def test_reconcile_rejects_duplicate_and_orphan_masks(self):
        prefix = "datasets/study_ct_v1"
        cases = {
            "duplicate": {
                f"{prefix}/source/masks/case001_mask.nii.gz": b"one",
                f"{prefix}/source/masks/case001_seg.nii.gz": b"two",
            },
            "orphan": {
                f"{prefix}/source/masks/unknown_mask.nii.gz": b"one",
            },
        }
        for name, mask_objects in cases.items():
            with self.subTest(name=name):
                objects = self._published_objects(
                    prefix, [("case001", "patient-a")])
                original_manifest = objects[f"{prefix}/manifest.yaml"]
                objects[f"{prefix}/source/images/case001.nii.gz"] = b"image"
                objects.update(mask_objects)
                s3 = FakeS3(objects)
                controller.get_s3 = lambda: s3

                with self.assertRaisesRegex(
                        ValueError,
                        "duplicate sample_id|no matching image sample_id"):
                    controller.reconcile_dataset("study_ct_v1")

                self.assertEqual(
                    s3.objects[f"{prefix}/manifest.yaml"], original_manifest)
                self.assertNotIn(
                    f"{prefix}/{controller.PUBLISH_LOCK}", s3.objects)

    def test_reconcile_fails_closed_on_corrupt_metadata(self):
        prefix = "datasets/study_ct_v1"
        objects = self._published_objects(prefix, [("case001", "patient-a")])
        original_manifest = objects[f"{prefix}/manifest.yaml"]
        objects[f"{prefix}/metadata/samples.parquet"] = b"not parquet"
        objects[f"{prefix}/source/images/case001.nii.gz"] = b"image"
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        with self.assertRaisesRegex(ValueError, "metadata is corrupt"):
            controller.reconcile_dataset("study_ct_v1")

        self.assertEqual(s3.objects[f"{prefix}/manifest.yaml"], original_manifest)
        self.assertEqual(
            s3.objects[f"{prefix}/metadata/samples.parquet"], b"not parquet"
        )

    def test_reconcile_rejects_public_label_level_matching_patient_id(self):
        prefix = "datasets/study_ct_v1"
        objects = self._published_objects(
            prefix,
            [("case001", "patient-a")],
            labels=["case"],
            label_levels=["case", "patient-a"],
        )
        objects[f"{prefix}/source/images/case001.nii.gz"] = b"image"
        before = dict(objects)
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3

        with self.assertRaisesRegex(
                ValueError, "must not equal sample or patient identifiers"):
            controller.reconcile_dataset("study_ct_v1")

        self.assertEqual(s3.objects, before)

    def _published_objects(self, prefix, rows, title=None, *, labels=None,
                           label_levels=None):
        dataset_id = prefix.rsplit("/", 1)[-1]
        label_col = "diagnosis" if labels is not None else None
        manifest = controller.generate_manifest(
            dataset_id, prefix, "ct", privacy_unit_col="patient_id",
            label_col=label_col, label_levels=label_levels,
        )
        if title:
            manifest["title"] = title
        columns = {
            "sample_id": [sample_id for sample_id, _ in rows],
            "source_kind": ["single_file"] * len(rows),
            "n_files": pa.array([1] * len(rows), type=pa.int32()),
            "patient_id": [patient_id for _, patient_id in rows],
        }
        if labels is not None:
            columns["diagnosis"] = labels
        table = pa.table(columns)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        return {
            f"{prefix}/manifest.yaml": yaml.safe_dump(
                manifest, sort_keys=False
            ).encode("utf-8"),
            f"{prefix}/metadata/samples.parquet": sink.getvalue().to_pybytes(),
        }


if __name__ == "__main__":
    unittest.main()
