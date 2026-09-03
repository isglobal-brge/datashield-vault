import http.client
import importlib.util
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "store_controller_http", ROOT / "controller" / "controller.py"
)
controller = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(controller)


class ControllerHttpTests(unittest.TestCase):
    def setUp(self):
        self.old_token = controller.OPERATOR_TOKEN
        self.old_webhook_token = controller.WEBHOOK_TOKEN
        self.old_limit = controller.MAX_WEBHOOK_BODY_BYTES
        self.old_bucket = controller.BUCKET
        controller.OPERATOR_TOKEN = ""
        controller.WEBHOOK_TOKEN = "webhook-token"
        controller.MAX_WEBHOOK_BODY_BYTES = 1024 * 1024
        controller.dirty_datasets.clear()
        controller.last_reconcile.clear()
        controller.last_errors.clear()
        self.server = controller.HTTPServer(
            ("127.0.0.1", 0), controller.Handler)
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        controller.OPERATOR_TOKEN = self.old_token
        controller.WEBHOOK_TOKEN = self.old_webhook_token
        controller.MAX_WEBHOOK_BODY_BYTES = self.old_limit
        controller.BUCKET = self.old_bucket
        controller.dirty_datasets.clear()
        controller.last_reconcile.clear()
        controller.last_errors.clear()

    def request(self, method, path, *, token=None, body=b""):
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.server.server_port, timeout=2)
        headers = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        payload = response.read()
        connection.close()
        return response.status, json.loads(payload) if payload else None

    def test_health_is_coarse_and_does_not_require_operator_token(self):
        controller.OPERATOR_TOKEN = "private-token"
        controller.BUCKET = "private-bucket"
        controller.dirty_datasets.add("private-dataset")

        status, payload = self.request("GET", "/health")

        self.assertEqual(status, 200)
        self.assertEqual(payload, {"status": "ok"})

    def test_operator_endpoints_are_disabled_without_a_token(self):
        get_status, get_payload = self.request("GET", "/datasets")
        post_status, post_payload = self.request(
            "POST", "/reconcile/private-dataset")

        self.assertEqual((get_status, post_status), (404, 404))
        self.assertEqual(get_payload, {"error": "not found"})
        self.assertEqual(post_payload, {"error": "not found"})

    def test_inventory_requires_bearer_token(self):
        controller.OPERATOR_TOKEN = "operator-token"
        inventory = [{
            "dataset_id": "study",
            "status": "published",
            "dirty": False,
            "last_reconcile_at": None,
            "has_error": False,
        }]
        with patch.object(controller, "list_datasets", return_value=inventory):
            denied_status, denied = self.request("GET", "/datasets")
            allowed_status, allowed = self.request(
                "GET", "/datasets", token="operator-token")

        self.assertEqual(denied_status, 403)
        self.assertEqual(denied, {"error": "forbidden"})
        self.assertEqual(allowed_status, 200)
        self.assertEqual(allowed, {"datasets": inventory})

    def test_reconcile_response_omits_dataset_and_counts(self):
        controller.OPERATOR_TOKEN = "operator-token"
        with patch.object(
                controller, "reconcile_dataset", return_value=(123, 45)):
            status, payload = self.request(
                "POST", "/reconcile/study", token="operator-token")

        self.assertEqual(status, 200)
        self.assertEqual(payload, {"status": "ok"})

    def test_reconcile_failure_does_not_return_internal_error(self):
        controller.OPERATOR_TOKEN = "operator-token"
        with patch.object(
                controller, "reconcile_dataset",
                side_effect=RuntimeError("private bucket/path detail")):
            with self.assertLogs(controller.log, level="ERROR") as captured:
                status, payload = self.request(
                    "POST", "/reconcile/study", token="operator-token")

        self.assertEqual(status, 500)
        self.assertEqual(payload, {"error": "reconciliation failed"})
        self.assertNotIn("private", json.dumps(payload))
        self.assertNotIn("study", json.dumps(payload))
        self.assertNotIn("private bucket/path detail", "\n".join(captured.output))

    def test_webhook_body_is_bounded_and_response_is_generic(self):
        controller.MAX_WEBHOOK_BODY_BYTES = 8
        status, payload = self.request(
            "POST", "/webhook/minio", token="webhook-token",
            body=b"0123456789")

        self.assertEqual(status, 413)
        self.assertEqual(payload, {"error": "request too large"})

    def test_valid_webhook_does_not_reflect_dataset_name(self):
        event = json.dumps({
            "Records": [{
                "eventName": "ObjectCreated:Put",
                "s3": {"object": {
                    "key": "datasets/study/source/images/case001.nii.gz",
                }},
            }],
        }).encode("utf-8")

        status, payload = self.request(
            "POST", "/webhook/minio", token="webhook-token", body=event)

        self.assertEqual(status, 200)
        self.assertEqual(payload, {"status": "ok"})
        self.assertNotIn("study", json.dumps(payload))
        self.assertIn("study", controller.dirty_datasets)

    def test_webhook_requires_its_dedicated_bearer(self):
        event = json.dumps({"Records": []}).encode("utf-8")

        missing_status, missing = self.request(
            "POST", "/webhook/minio", body=event)
        wrong_status, wrong = self.request(
            "POST", "/webhook/minio", token="wrong", body=event)
        non_ascii_status, non_ascii = self.request(
            "POST", "/webhook/minio", token="é", body=event)
        allowed_status, allowed = self.request(
            "POST", "/webhook/minio", token="webhook-token", body=event)

        self.assertEqual(
            (missing_status, wrong_status, non_ascii_status, allowed_status),
            (403, 403, 403, 200),
        )
        self.assertEqual(missing, {"error": "forbidden"})
        self.assertEqual(wrong, {"error": "forbidden"})
        self.assertEqual(non_ascii, {"error": "forbidden"})
        self.assertEqual(allowed, {"status": "ok"})


if __name__ == "__main__":
    unittest.main()
