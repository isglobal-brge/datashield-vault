import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "store_controller_sqs", ROOT / "controller" / "controller.py"
)
controller = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(controller)


class FakeSQS:
    def __init__(self, body):
        self.body = body
        self.deleted = []

    def receive_message(self, QueueUrl, MaxNumberOfMessages, WaitTimeSeconds):
        return {
            "Messages": [{
                "Body": self.body,
                "ReceiptHandle": "receipt-1",
            }]
        }

    def delete_message(self, QueueUrl, ReceiptHandle):
        self.deleted.append((QueueUrl, ReceiptHandle))


class SqsTests(unittest.TestCase):
    def setUp(self):
        self.old_dirty = set(controller.dirty_datasets)
        controller.dirty_datasets.clear()

    def tearDown(self):
        controller.dirty_datasets.clear()
        controller.dirty_datasets.update(self.old_dirty)

    def test_sqs_poll_mode_processes_s3_event_through_shared_path(self):
        event = {
            "Records": [{
                "eventName": "ObjectCreated:Put",
                "s3": {"object": {"key": "datasets/lung_ct/source/images/case001.nii.gz"}},
            }]
        }
        sqs = FakeSQS(json.dumps(event))
        processed = controller.process_sqs_messages(
            sqs, "https://sqs.example/dsimaging", wait_time_seconds=0)

        self.assertEqual(processed, 1)
        self.assertEqual(sqs.deleted, [("https://sqs.example/dsimaging", "receipt-1")])
        self.assertIn("lung_ct", controller.dirty_datasets)

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


if __name__ == "__main__":
    unittest.main()
