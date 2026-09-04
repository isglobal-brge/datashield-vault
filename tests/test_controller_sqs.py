import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "store_controller_sqs", ROOT / "controller" / "controller.py"
)
controller = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(controller)


class FakeSQS:
    def __init__(self, body, operations=None):
        self.body = body
        self.deleted = []
        self.operations = operations

    def receive_message(self, QueueUrl, MaxNumberOfMessages, WaitTimeSeconds):
        return {
            "Messages": [{
                "Body": self.body,
                "ReceiptHandle": "receipt-1",
            }]
        }

    def delete_message(self, QueueUrl, ReceiptHandle):
        if self.operations is not None:
            self.operations.append("delete-message")
        self.deleted.append((QueueUrl, ReceiptHandle))


class FakeMarkerS3:
    def __init__(self, operations=None):
        self.objects = {}
        self.operations = operations

    def get_bucket_versioning(self, Bucket):
        if self.operations is not None:
            self.operations.append("check-versioning")
        return {"Status": "Enabled"}

    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.operations is not None:
            self.operations.append("put-marker")
        self.objects[Key] = bytes(Body)
        return {"VersionId": f"version-{Key}"}


class SqsTests(unittest.TestCase):
    def setUp(self):
        self.old_dirty = set(controller.dirty_datasets)
        self.old_get_s3 = controller.get_s3
        controller.dirty_datasets.clear()
        self.s3 = FakeMarkerS3()
        controller.get_s3 = lambda: self.s3

    def tearDown(self):
        controller.dirty_datasets.clear()
        controller.dirty_datasets.update(self.old_dirty)
        controller.get_s3 = self.old_get_s3

    def test_sqs_poll_mode_processes_s3_event_through_shared_path(self):
        event = {
            "Records": [{
                "eventName": "ObjectCreated:Put",
                "s3": {"object": {"key": "datasets/lung_ct/source/images/case001.nii.gz"}},
            }]
        }
        operations = []
        sqs = FakeSQS(json.dumps(event), operations)
        self.s3.operations = operations
        with self.assertLogs(controller.log, level="INFO") as captured:
            processed = controller.process_sqs_messages(
                sqs, "https://sqs.example/dsimaging", wait_time_seconds=0)

        self.assertEqual(processed, 1)
        self.assertEqual(sqs.deleted, [("https://sqs.example/dsimaging", "receipt-1")])
        self.assertIn("lung_ct", controller.dirty_datasets)
        self.assertNotIn("case001", "\n".join(captured.output))
        self.assertEqual(
            operations,
            ["check-versioning", "put-marker", "delete-message"],
        )
        self.assertEqual(len(self.s3.objects), 1)

    def test_sns_wrapped_s3_event_is_supported(self):
        event = {
            "Records": [{
                "eventName": "ObjectRemoved:Delete",
                "s3": {"object": {"key": "datasets/lung_ct/source/masks/case001.nii.gz"}},
            }]
        }
        sqs = FakeSQS(json.dumps({"Message": json.dumps(event)}))
        controller.process_sqs_messages(
            sqs, "https://sqs.example/dsimaging", wait_time_seconds=0)

        self.assertIn("lung_ct", controller.dirty_datasets)

    def test_event_with_invalid_dataset_id_is_ignored(self):
        event = {
            "Records": [{
                "eventName": "ObjectCreated:Put",
                "s3": {"object": {
                    "key": "datasets/../source/images/case001.nii.gz",
                }},
            }]
        }

        self.assertEqual(controller.handle_s3_event_payload(event), 0)
        self.assertEqual(controller.dirty_datasets, set())

    def test_malformed_record_does_not_discard_valid_record(self):
        event = {
            "Records": [{
                "eventName": "ObjectCreated:Put",
                "s3": {"object": {
                    "key": "datasets/lung_ct/source/images/case001.nii.gz",
                }},
            }, None],
        }
        sqs = FakeSQS(json.dumps(event))

        processed = controller.process_sqs_messages(
            sqs, "https://sqs.example/dsimaging", wait_time_seconds=0)

        self.assertEqual(processed, 1)
        self.assertIn("lung_ct", controller.dirty_datasets)
        self.assertEqual(len(self.s3.objects), 1)
        self.assertEqual(
            sqs.deleted,
            [("https://sqs.example/dsimaging", "receipt-1")],
        )

    def test_malformed_message_is_discarded_instead_of_wedging_batch(self):
        sqs = FakeSQS("{not json")

        with self.assertLogs(controller.log, level="WARNING") as captured:
            processed = controller.process_sqs_messages(
                sqs, "https://sqs.example/dsimaging", wait_time_seconds=0)

        self.assertEqual(processed, 1)
        self.assertEqual(
            sqs.deleted,
            [("https://sqs.example/dsimaging", "receipt-1")],
        )
        self.assertIn("malformed SQS notification", "\n".join(captured.output))
        self.assertEqual(controller.dirty_datasets, set())

    def test_excessively_nested_message_is_discarded(self):
        sqs = FakeSQS("[" * 2000 + "]" * 2000)

        with self.assertLogs(controller.log, level="WARNING"):
            processed = controller.process_sqs_messages(
                sqs, "https://sqs.example/dsimaging", wait_time_seconds=0)

        self.assertEqual(processed, 1)
        self.assertEqual(
            sqs.deleted,
            [("https://sqs.example/dsimaging", "receipt-1")],
        )

    def test_transient_processing_failure_keeps_message_for_retry(self):
        sqs = FakeSQS(json.dumps({"Records": []}))

        with patch.object(
                controller, "persist_dirty_datasets",
                side_effect=RuntimeError("temporary failure")):
            with self.assertRaisesRegex(RuntimeError, "temporary failure"):
                controller.process_sqs_messages(
                    sqs, "https://sqs.example/dsimaging",
                    wait_time_seconds=0,
                )

        self.assertEqual(sqs.deleted, [])

    def test_marker_write_failure_keeps_valid_message_for_retry(self):
        event = {
            "Records": [{
                "eventName": "ObjectCreated:Put",
                "s3": {"object": {
                    "key": "datasets/lung_ct/source/images/case001.nii.gz",
                }},
            }],
        }
        sqs = FakeSQS(json.dumps(event))

        with patch.object(
                controller, "put_dirty_marker",
                side_effect=RuntimeError("storage unavailable")):
            with self.assertRaisesRegex(RuntimeError, "storage unavailable"):
                controller.process_sqs_messages(
                    sqs, "https://sqs.example/dsimaging",
                    wait_time_seconds=0,
                )

        self.assertEqual(sqs.deleted, [])
        self.assertEqual(controller.dirty_datasets, set())

    def test_sqs_client_initialization_retries_with_backoff(self):
        sqs = FakeSQS(json.dumps({"Records": []}))
        with patch.object(controller, "SQS_QUEUE_URL", "queue-url"), \
                patch.object(
                    controller, "get_sqs",
                    side_effect=[RuntimeError("temporarily unavailable"), sqs],
                ) as get_sqs, patch.object(
                    controller, "process_sqs_messages",
                    side_effect=KeyboardInterrupt,
                ), patch.object(controller.time, "sleep") as sleep:
            with self.assertLogs(controller.log, level="ERROR"):
                with self.assertRaises(KeyboardInterrupt):
                    controller.sqs_loop()

        self.assertEqual(get_sqs.call_count, 2)
        sleep.assert_called_once_with(1)


if __name__ == "__main__":
    unittest.main()
