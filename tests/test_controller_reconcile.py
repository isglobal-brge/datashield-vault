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
        self.fail_delete_key = None
        self.upload_history = []
        self.get_requests = []
        self.delete_history = []

    def get_bucket_versioning(self, Bucket):
        return {"Status": "Enabled"}

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
        digest = hashlib.md5(value).hexdigest()
        return {
            "ETag": f'"{digest}"',
            "VersionId": f"version-{digest}",
        }

    def delete_object(self, Bucket, Key, IfMatch=None, VersionId=None):
        if Key == self.fail_delete_key:
            raise RuntimeError("injected delete failure")
        if Key not in self.objects:
            raise KeyError(Key)
        etag = f'"{hashlib.md5(self.objects[Key]).hexdigest()}"'
        if IfMatch is not None and IfMatch != etag:
            raise RuntimeError("precondition failed")
        expected_version = f"version-{hashlib.md5(self.objects[Key]).hexdigest()}"
        if VersionId is not None and VersionId != expected_version:
            raise RuntimeError("wrong version")
        self.delete_history.append({
            "Bucket": Bucket,
            "Key": Key,
            "IfMatch": IfMatch,
            "VersionId": VersionId,
        })
        del self.objects[Key]
        return {}

    def delete_objects(self, Bucket, Delete):
        for item in Delete.get("Objects", []):
            self.objects.pop(item["Key"], None)
        return {}


class ReconcileTests(unittest.TestCase):
    def test_dataset_id_contract_matches_admin(self):
        self.assertTrue(controller.is_valid_dataset_id("study_ct-v1.2"))
        self.assertTrue(controller.is_valid_dataset_id("a" * 128))
        for dataset_id in (
                "", "Study", "study..v1", "a" * 129, None, b"study"):
            with self.subTest(dataset_id=repr(dataset_id)[:24]):
                self.assertFalse(controller.is_valid_dataset_id(dataset_id))

    def test_startup_recovery_enqueues_only_canonical_dirty_markers(self):
        s3 = FakeS3({
            f"{controller.DIRTY_PREFIX}study_a/{'a' * 32}": b"marker",
            f"{controller.DIRTY_PREFIX}study_b/{'b' * 32}": b"marker",
            f"{controller.DIRTY_PREFIX}Study_C/{'c' * 32}": b"invalid-id",
            f"{controller.DIRTY_PREFIX}study..d/{'d' * 32}": b"invalid-dots",
            f"{controller.DIRTY_PREFIX}{'e' * 129}/{'e' * 32}": b"too-long",
            f"{controller.DIRTY_PREFIX}study_d/not-a-marker": b"invalid-marker",
            "datasets/study_a/manifest.yaml": b"manifest",
            "datasets/unmarked/source/images/case001.nii.gz": b"image",
        })
        dirty = {"event_received_during_startup"}
        with patch.object(controller, "get_s3", return_value=s3), \
                patch.object(controller, "dirty_datasets", dirty):
            recovered = controller.enqueue_persisted_dirty_datasets()

        self.assertEqual(recovered, 2)
        self.assertEqual(
            dirty,
            {"event_received_during_startup", "study_a", "study_b"},
        )

    def test_startup_recovery_retries_with_backoff(self):
        with patch.object(
                controller, "migrate_legacy_pending_datasets",
                side_effect=[RuntimeError("storage unavailable"), 0],
        ) as migrate, patch.object(
                controller, "enqueue_persisted_dirty_datasets", return_value=2,
        ) as enqueue, patch.object(
                controller, "reconcile_loop", side_effect=StopIteration,
        ), patch.object(controller.time, "sleep") as sleep:
            with self.assertLogs(controller.log, level="ERROR"):
                with self.assertRaises(StopIteration):
                    controller.recover_then_reconcile()

        self.assertEqual(migrate.call_count, 2)
        enqueue.assert_called_once_with()
        sleep.assert_called_once_with(1)

    def test_legacy_queue_migration_is_one_shot(self):
        s3 = FakeS3({
            "datasets/study_a/manifest.yaml": b"manifest",
            "datasets/study_b/source/images/case001.nii.gz": b"image",
            "datasets/Study_C/manifest.yaml": b"invalid-id",
            "datasets/study..d/manifest.yaml": b"invalid-dots",
            f"datasets/{'e' * 129}/manifest.yaml": b"too-long",
        })
        with patch.object(controller, "get_s3", return_value=s3):
            migrated = controller.migrate_legacy_pending_datasets()
            object_count = len(s3.objects)
            migrated_again = controller.migrate_legacy_pending_datasets()

        self.assertEqual(migrated, 2)
        self.assertEqual(migrated_again, 0)
        self.assertEqual(len(s3.objects), object_count)
        self.assertIn(controller.DIRTY_MIGRATION_KEY, s3.objects)
        self.assertIn(
            controller.dirty_marker_key(
                "study_a", controller.DIRTY_MIGRATION_MARKER_ID),
            s3.objects,
        )
        self.assertIn(
            controller.dirty_marker_key(
                "study_b", controller.DIRTY_MIGRATION_MARKER_ID),
            s3.objects,
        )

    def test_legacy_queue_migration_recovers_after_partial_failure(self):
        marker_b = controller.dirty_marker_key(
            "study_b", controller.DIRTY_MIGRATION_MARKER_ID)

        class FailOnceS3(FakeS3):
            def __init__(self, objects):
                super().__init__(objects)
                self.failed = False

            def put_object(self, Bucket, Key, Body, **kwargs):
                if Key == marker_b and not self.failed:
                    self.failed = True
                    raise RuntimeError("injected migration failure")
                return super().put_object(Bucket, Key, Body, **kwargs)

        s3 = FailOnceS3({
            "datasets/study_a/manifest.yaml": b"manifest",
            "datasets/study_b/manifest.yaml": b"manifest",
        })
        marker_a = controller.dirty_marker_key(
            "study_a", controller.DIRTY_MIGRATION_MARKER_ID)
        with patch.object(controller, "get_s3", return_value=s3):
            with self.assertRaisesRegex(RuntimeError, "migration failure"):
                controller.migrate_legacy_pending_datasets()
            self.assertIn(marker_a, s3.objects)
            self.assertNotIn(controller.DIRTY_MIGRATION_KEY, s3.objects)

            self.assertEqual(controller.migrate_legacy_pending_datasets(), 2)

        self.assertIn(marker_a, s3.objects)
        self.assertIn(marker_b, s3.objects)
        self.assertIn(controller.DIRTY_MIGRATION_KEY, s3.objects)

    def test_listener_is_bound_before_recovery_worker_starts(self):
        events = []

        class FakeThread:
            def __init__(self, target, daemon):
                self.target = target

            def start(self):
                events.append(f"start:{self.target.__name__}")

        class FakeServer:
            def __init__(self, *args):
                events.append("bind")

            def serve_forever(self):
                events.append("serve")
                raise KeyboardInterrupt()

            def server_close(self):
                events.append("close")

        with patch.object(controller.threading, "Thread", FakeThread), \
                patch.object(controller, "HTTPServer", FakeServer), \
                patch.object(controller, "SQS_QUEUE_URL", ""):
            controller.main()

        self.assertEqual(
            events,
            ["bind", "start:recover_then_reconcile", "serve", "close"],
        )

    def test_running_worker_discovers_markers_from_another_replica(self):
        dataset_id = "study_a"
        marker = controller.dirty_marker_key(dataset_id, "a" * 32)
        s3 = FakeS3({marker: b"marker"})
        dirty = set()

        with patch.multiple(
                controller,
                get_s3=lambda: s3,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors={},
        ), patch.object(
                controller, "reconcile_pending_dataset", return_value=(1, 0),
        ) as reconcile, patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertRaises(StopIteration):
                controller.reconcile_loop()

        reconcile.assert_called_once_with(dataset_id)

    def test_success_telemetry_does_not_retain_exact_collection_counts(self):
        controller.last_reconcile.clear()

        controller.record_success("study_ct_v1", 6, 2)

        self.assertEqual(
            set(controller.last_reconcile["study_ct_v1"]), {"at"})

    def test_reconcile_rejects_invalid_dataset_id_before_storage_access(self):
        for dataset_id in ("../other", "study..v1", "a" * 129):
            with self.subTest(dataset_id=dataset_id[:20]), \
                    patch.object(controller, "get_s3") as get_s3:
                with self.assertRaisesRegex(ValueError, "invalid dataset id"):
                    controller.reconcile_dataset(dataset_id)
                get_s3.assert_not_called()

    def test_reconcile_loop_does_not_requeue_fully_deleted_dataset(self):
        dataset_id = "study_ct_v1"
        s3 = FakeS3({})
        controller.get_s3 = lambda: s3
        dirty = {dataset_id}
        successes = {}
        failures = {dataset_id: {"at": "previous failure"}}

        with patch.multiple(
                controller,
                dirty_datasets=dirty,
                last_reconcile=successes,
                last_errors=failures,
        ), patch.object(
                controller, "record_success", wraps=controller.record_success,
        ) as record_success, patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertRaises(StopIteration):
                controller.reconcile_loop()

        record_success.assert_called_once_with(dataset_id, 0, 0)
        self.assertEqual(dirty, set())
        self.assertIn(dataset_id, successes)
        self.assertEqual(failures, {})
        self.assertEqual(s3.objects, {})

    def test_reconcile_loop_does_not_retry_missing_manifest(self):
        dataset_id = "study_ct_v1"
        prefix = f"datasets/{dataset_id}"
        leftover_key = f"{prefix}/unmanaged-object"
        marker_key = controller.dirty_marker_key(dataset_id, "a" * 32)
        s3 = FakeS3({leftover_key: b"leftover", marker_key: b"marker"})
        controller.get_s3 = lambda: s3
        dirty = {dataset_id}
        successes = {}
        failures = {}

        with patch.multiple(
                controller,
                dirty_datasets=dirty,
                last_reconcile=successes,
                last_errors=failures,
        ), patch.object(
                controller, "record_failure", wraps=controller.record_failure,
        ) as record_failure, patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertLogs(controller.log, level="ERROR"):
                with self.assertRaises(StopIteration):
                    controller.reconcile_loop()

        failure = record_failure.call_args.args[1]
        self.assertIsInstance(failure, controller.InvalidDatasetContent)
        self.assertIn("manifest is missing", str(failure))
        self.assertEqual(record_failure.call_args.kwargs, {"retry": False})
        self.assertEqual(dirty, set())
        self.assertEqual(successes, {})
        self.assertIn(dataset_id, failures)
        self.assertEqual(s3.objects, {leftover_key: b"leftover"})

    def test_reconcile_loop_does_not_retry_invalid_source_and_releases_lock(self):
        dataset_id = "study_ct_v1"
        prefix = f"datasets/{dataset_id}"
        objects = self._published_objects(
            prefix, [("case001", "patient-a")])
        objects.update({
            f"{prefix}/source/images/site-a/case001.nii.gz": b"one",
            f"{prefix}/source/images/site-b/case001.nii.gz": b"two",
        })
        s3 = FakeS3(objects)
        controller.get_s3 = lambda: s3
        dirty = {dataset_id}
        failures = {}

        with patch.multiple(
                controller,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors=failures,
        ), patch.object(
                controller, "record_failure", wraps=controller.record_failure,
        ) as record_failure, patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertLogs(controller.log, level="ERROR"):
                with self.assertRaises(StopIteration):
                    controller.reconcile_loop()

        failure = record_failure.call_args.args[1]
        self.assertIsInstance(failure, controller.InvalidSourceContent)
        self.assertEqual(record_failure.call_args.kwargs, {"retry": False})
        self.assertEqual(dirty, set())
        self.assertIn(dataset_id, failures)
        self.assertNotIn(f"{prefix}/{controller.PUBLISH_LOCK}", s3.objects)
        lock_delete = next(
            call for call in s3.delete_history
            if call["Key"] == f"{prefix}/{controller.PUBLISH_LOCK}")
        self.assertIsNotNone(lock_delete["VersionId"])
        self.assertIsNone(lock_delete["IfMatch"])

    def test_failed_lock_release_retains_marker_after_invalid_content(self):
        dataset_id = "study_ct_v1"
        prefix = f"datasets/{dataset_id}"
        lock_key = f"{prefix}/{controller.PUBLISH_LOCK}"
        marker_key = controller.dirty_marker_key(dataset_id, "a" * 32)
        objects = self._published_objects(
            prefix, [("case001", "patient-a")])
        objects.update({
            marker_key: b"marker",
            f"{prefix}/source/images/site-a/case001.nii.gz": b"one",
            f"{prefix}/source/images/site-b/case001.nii.gz": b"two",
        })
        s3 = FakeS3(objects)
        s3.fail_delete_key = lock_key
        dirty = {dataset_id}
        failures = {}

        with patch.multiple(
                controller,
                get_s3=lambda: s3,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors=failures,
        ), patch.object(
                controller, "record_failure", wraps=controller.record_failure,
        ) as record_failure, patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertLogs(controller.log, level="ERROR"):
                with self.assertRaises(StopIteration):
                    controller.reconcile_loop()

        failure = record_failure.call_args.args[1]
        self.assertIsInstance(failure, controller.LockOwnershipLost)
        self.assertEqual(record_failure.call_args.kwargs, {})
        self.assertIn(marker_key, s3.objects)
        self.assertIn(lock_key, s3.objects)
        self.assertEqual(dirty, {dataset_id})

    def test_reconcile_loop_requeues_transient_failure(self):
        dataset_id = "study_ct_v1"
        s3 = FakeS3({})
        dirty = {dataset_id}
        failures = {}

        with patch.multiple(
                controller,
                get_s3=lambda: s3,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors=failures,
        ), patch.object(
                controller, "reconcile_dataset",
                side_effect=RuntimeError("temporary storage failure"),
        ), patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertLogs(controller.log, level="ERROR"):
                with self.assertRaises(StopIteration):
                    controller.reconcile_loop()

        self.assertEqual(dirty, {dataset_id})
        self.assertIn(dataset_id, failures)

    def test_success_clears_only_snapshotted_versioned_marker(self):
        dataset_id = "study_ct_v1"
        old_marker = controller.dirty_marker_key(dataset_id, "a" * 32)
        new_marker = controller.dirty_marker_key(dataset_id, "b" * 32)
        s3 = FakeS3({old_marker: b"old-marker"})
        dirty = {dataset_id}

        def reconcile_after_new_event(_dataset_id):
            s3.objects[new_marker] = b"new-marker"
            controller.mark_dirty(_dataset_id)
            return 0, 0

        with patch.multiple(
                controller,
                get_s3=lambda: s3,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors={},
        ), patch.object(
                controller, "reconcile_dataset",
                side_effect=reconcile_after_new_event,
        ), patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertRaises(StopIteration):
                controller.reconcile_loop()

        self.assertNotIn(old_marker, s3.objects)
        self.assertIn(new_marker, s3.objects)
        self.assertEqual(dirty, {dataset_id})
        marker_delete = next(
            call for call in s3.delete_history if call["Key"] == old_marker)
        self.assertEqual(
            marker_delete["VersionId"],
            f"version-{hashlib.md5(b'old-marker').hexdigest()}",
        )

    def test_invalid_content_clears_old_marker_but_keeps_concurrent_event(self):
        dataset_id = "study_ct_v1"
        old_marker = controller.dirty_marker_key(dataset_id, "a" * 32)
        new_marker = controller.dirty_marker_key(dataset_id, "b" * 32)
        s3 = FakeS3({old_marker: b"old-marker"})
        dirty = {dataset_id}

        def fail_after_new_event(_dataset_id):
            s3.objects[new_marker] = b"new-marker"
            controller.mark_dirty(_dataset_id)
            raise controller.InvalidDatasetContent("invalid content")

        with patch.multiple(
                controller,
                get_s3=lambda: s3,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors={},
        ), patch.object(
                controller, "reconcile_dataset", side_effect=fail_after_new_event,
        ), patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertLogs(controller.log, level="ERROR"):
                with self.assertRaises(StopIteration):
                    controller.reconcile_loop()

        self.assertNotIn(old_marker, s3.objects)
        self.assertIn(new_marker, s3.objects)
        self.assertEqual(dirty, {dataset_id})

    def test_transient_failure_retains_durable_marker(self):
        dataset_id = "study_ct_v1"
        marker = controller.dirty_marker_key(dataset_id, "a" * 32)
        s3 = FakeS3({marker: b"marker"})
        dirty = {dataset_id}

        with patch.multiple(
                controller,
                get_s3=lambda: s3,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors={},
        ), patch.object(
                controller, "reconcile_dataset",
                side_effect=RuntimeError("temporary storage failure"),
        ), patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertLogs(controller.log, level="ERROR"):
                with self.assertRaises(StopIteration):
                    controller.reconcile_loop()

        self.assertIn(marker, s3.objects)
        self.assertEqual(dirty, {dataset_id})

    def test_transient_manifest_read_failure_is_not_content_validation(self):
        dataset_id = "study_ct_v1"
        prefix = f"datasets/{dataset_id}"
        marker = controller.dirty_marker_key(dataset_id, "a" * 32)
        manifest_key = f"{prefix}/manifest.yaml"

        class FailingHeadS3(FakeS3):
            def head_object(self, Bucket, Key):
                if Key == manifest_key:
                    raise RuntimeError("temporary storage failure")
                return super().head_object(Bucket, Key)

        objects = self._published_objects(
            prefix, [("case001", "patient-a")])
        objects[marker] = b"marker"
        s3 = FailingHeadS3(objects)
        dirty = {dataset_id}

        with patch.multiple(
                controller,
                get_s3=lambda: s3,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors={},
        ), patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertLogs(controller.log, level="ERROR"):
                with self.assertRaises(StopIteration):
                    controller.reconcile_loop()

        self.assertIn(marker, s3.objects)
        self.assertEqual(dirty, {dataset_id})

    def test_unversioned_store_is_rejected_before_marker_or_lock_creation(self):
        dataset_id = "study_ct_v1"
        class UnversionedS3(FakeS3):
            def get_bucket_versioning(self, Bucket):
                return {}

        s3 = UnversionedS3({})
        with patch.object(s3, "put_object", wraps=s3.put_object) as put_object:
            with self.assertRaisesRegex(RuntimeError, "versioning must be enabled"):
                controller.persist_dirty_marker(s3, dataset_id)
            with self.assertRaisesRegex(RuntimeError, "versioning must be enabled"):
                controller.acquire_publish_lock(s3, f"datasets/{dataset_id}")

        put_object.assert_not_called()

    def test_marker_without_version_is_retained_and_not_reconciled(self):
        dataset_id = "study_ct_v1"
        marker = controller.dirty_marker_key(dataset_id, "a" * 32)

        class MissingMarkerVersionS3(FakeS3):
            def head_object(self, Bucket, Key):
                response = super().head_object(Bucket, Key)
                if Key == marker:
                    response.pop("VersionId", None)
                return response

        s3 = MissingMarkerVersionS3({marker: b"marker"})
        with patch.object(controller, "get_s3", return_value=s3), \
                patch.object(controller, "reconcile_dataset") as reconcile:
            with self.assertRaisesRegex(RuntimeError, "no usable version ID"):
                controller.reconcile_pending_dataset(dataset_id)

        reconcile.assert_not_called()
        self.assertIn(marker, s3.objects)

    def test_lock_missing_response_version_is_adopted_from_stable_head(self):
        dataset_id = "study_ct_v1"
        prefix = f"datasets/{dataset_id}"
        lock_key = f"{prefix}/{controller.PUBLISH_LOCK}"

        class MissingLockVersionS3(FakeS3):
            def put_object(self, Bucket, Key, Body, **kwargs):
                response = super().put_object(Bucket, Key, Body, **kwargs)
                if Key == lock_key:
                    response.pop("VersionId", None)
                return response

        s3 = MissingLockVersionS3({})
        publish_lock = controller.acquire_publish_lock(s3, prefix)

        self.assertEqual(publish_lock["key"], lock_key)
        self.assertTrue(controller.publish_lock_is_owned(s3, publish_lock))
        self.assertTrue(controller.release_publish_lock(s3, publish_lock))
        self.assertNotIn(lock_key, s3.objects)

    def test_lock_without_observable_version_fails_closed(self):
        dataset_id = "study_ct_v1"
        prefix = f"datasets/{dataset_id}"
        lock_key = f"{prefix}/{controller.PUBLISH_LOCK}"

        class MissingLockVersionS3(FakeS3):
            def put_object(self, Bucket, Key, Body, **kwargs):
                response = super().put_object(Bucket, Key, Body, **kwargs)
                if Key == lock_key:
                    response.pop("VersionId", None)
                return response

            def head_object(self, Bucket, Key):
                response = super().head_object(Bucket, Key)
                if Key == lock_key:
                    response.pop("VersionId", None)
                return response

        s3 = MissingLockVersionS3({})
        with self.assertRaisesRegex(
                controller.LockOwnershipLost,
                "versioned reconciliation lock was not established"):
            controller.acquire_publish_lock(s3, prefix)

        self.assertIn(lock_key, s3.objects)

    def test_lock_is_adopted_when_put_commits_but_response_is_lost(self):
        dataset_id = "study_ct_v1"
        prefix = f"datasets/{dataset_id}"
        lock_key = f"{prefix}/{controller.PUBLISH_LOCK}"

        class CommitThenFailS3(FakeS3):
            def __init__(self):
                super().__init__({})
                self.failed = False

            def put_object(self, Bucket, Key, Body, **kwargs):
                response = super().put_object(Bucket, Key, Body, **kwargs)
                if Key == lock_key and not self.failed:
                    self.failed = True
                    raise RuntimeError("response lost after commit")
                return response

        s3 = CommitThenFailS3()
        publish_lock = controller.acquire_publish_lock(s3, prefix)

        self.assertEqual(publish_lock["key"], lock_key)
        self.assertTrue(controller.publish_lock_is_owned(s3, publish_lock))
        self.assertTrue(controller.release_publish_lock(s3, publish_lock))
        self.assertNotIn(lock_key, s3.objects)

    def test_event_during_validation_failure_remains_dirty(self):
        dataset_id = "study_ct_v1"
        s3 = FakeS3({})
        dirty = {dataset_id}

        def fail_after_event(_dataset_id):
            controller.mark_dirty(_dataset_id)
            raise controller.InvalidSourceContent("invalid dataset content")

        with patch.multiple(
                controller,
                get_s3=lambda: s3,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors={},
        ), patch.object(
                controller, "reconcile_dataset", side_effect=fail_after_event,
        ), patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertLogs(controller.log, level="ERROR"):
                with self.assertRaises(StopIteration):
                    controller.reconcile_loop()

        self.assertEqual(dirty, {dataset_id})

    def test_deleted_prefix_rechecks_source_after_first_prefix_listing(self):
        dataset_id = "study_ct_v1"
        prefix = f"datasets/{dataset_id}"
        late_key = f"{prefix}/source/images/late.nii.gz"
        s3 = FakeS3({})
        controller.get_s3 = lambda: s3
        original_list = controller.list_objects
        injected = False

        def list_then_inject(client, requested_prefix, **kwargs):
            nonlocal injected
            result = original_list(client, requested_prefix, **kwargs)
            if requested_prefix == f"{prefix}/" and not injected:
                s3.objects[late_key] = b"late-source"
                injected = True
            return result

        with patch.object(controller, "list_objects",
                          side_effect=list_then_inject), self.assertRaisesRegex(
                              RuntimeError, "source roster changed"):
            controller.reconcile_dataset(dataset_id)

        self.assertEqual(s3.objects, {late_key: b"late-source"})

    def test_deleted_prefix_rechecks_lock_ownership(self):
        dataset_id = "study_ct_v1"
        prefix = f"datasets/{dataset_id}"
        lock_key = f"{prefix}/{controller.PUBLISH_LOCK}"
        s3 = FakeS3({})
        controller.get_s3 = lambda: s3
        original_list = controller.list_objects
        replaced = False

        def list_then_replace_lock(client, requested_prefix, **kwargs):
            nonlocal replaced
            result = original_list(client, requested_prefix, **kwargs)
            if requested_prefix == f"{prefix}/" and not replaced:
                s3.objects[lock_key] = (
                    b'{"status":"publishing","owner":"replacement"}')
                replaced = True
            return result

        with patch.object(controller, "list_objects",
                          side_effect=list_then_replace_lock), self.assertRaisesRegex(
                              controller.LockOwnershipLost,
                              "ownership was lost"):
            controller.reconcile_dataset(dataset_id)

        self.assertEqual(
            s3.objects[lock_key],
            b'{"status":"publishing","owner":"replacement"}',
        )

    def test_event_arriving_during_reconcile_remains_dirty(self):
        dataset_id = "study_ct_v1"
        s3 = FakeS3({})
        dirty = {dataset_id}

        def reconcile_then_receive_event(_dataset_id):
            controller.mark_dirty(_dataset_id)
            return 0, 0

        with patch.multiple(
                controller,
                get_s3=lambda: s3,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors={},
        ), patch.object(
                controller, "reconcile_dataset",
                side_effect=reconcile_then_receive_event,
        ), patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertRaises(StopIteration):
                controller.reconcile_loop()

        self.assertEqual(dirty, {dataset_id})

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

    def test_deeply_nested_manifest_is_invalid_without_retry_loop(self):
        dataset_id = "study_ct_v1"
        prefix = f"datasets/{dataset_id}"
        marker_key = controller.dirty_marker_key(dataset_id, "a" * 32)
        objects = self._published_objects(
            prefix, [("case001", "patient-a")])
        objects[f"{prefix}/manifest.yaml"] = (
            ("[" * 1500) + (" ]" * 1500)
        ).encode("utf-8")
        objects[marker_key] = b"marker"
        s3 = FakeS3(objects)
        dirty = {dataset_id}

        with patch.multiple(
                controller,
                get_s3=lambda: s3,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors={},
        ), patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertLogs(controller.log, level="ERROR"):
                with self.assertRaises(StopIteration):
                    controller.reconcile_loop()

        self.assertEqual(dirty, set())
        self.assertNotIn(marker_key, s3.objects)
        self.assertNotIn(f"{prefix}/{controller.PUBLISH_LOCK}", s3.objects)

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

    def test_existing_lock_is_fail_closed_and_marker_survives(self):
        dataset_id = "study_ct_v1"
        prefix = f"datasets/{dataset_id}"
        lock_key = f"{prefix}/{controller.PUBLISH_LOCK}"
        marker_key = controller.dirty_marker_key(dataset_id, "a" * 32)
        s3 = FakeS3({
            lock_key: b'{"status":"reconciling","owner":"orphan-or-active"}',
            marker_key: b"marker",
        })
        dirty = {dataset_id}

        with patch.multiple(
                controller,
                get_s3=lambda: s3,
                dirty_datasets=dirty,
                last_reconcile={},
                last_errors={},
        ), patch.object(
                controller.time, "sleep", side_effect=StopIteration,
        ):
            with self.assertRaises(StopIteration):
                controller.reconcile_loop()

        self.assertIn(lock_key, s3.objects)
        self.assertIn(marker_key, s3.objects)
        self.assertEqual(dirty, {dataset_id})

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
            "datasets/study..c/manifest.yaml": b"invalid-dots",
            f"datasets/{'d' * 129}/manifest.yaml": b"too-long",
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
