#!/usr/bin/env python3
"""dsImagingStore controller.

Receives MinIO bucket notifications and reconciles dataset artifacts:
content_hash_index.parquet, mask hash indexes, sample_manifests.parquet,
samples.parquet and manifest.yaml. Direct uploads to
datasets/<id>/source/images/ and datasets/<id>/source/masks/ therefore
converge to the same layout produced by dsimaging-admin publish/rescan.
"""

import json
import hmac
import logging
import os
import re
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import unquote_plus

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from dsimaging_admin.manifest import (
    build_hash_index as core_build_hash_index,
    build_sample_manifests as core_build_sample_manifests,
    build_samples_metadata as core_build_samples_metadata,
    generate_manifest as core_generate_manifest,
    metadata_contract_from_manifest,
    scan_s3_images as core_scan_s3_images,
    scan_s3_masks as core_scan_s3_masks,
    validate_manifest_scope,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("controller")

SQS_QUEUE_URL = os.environ.get("DSIMAGING_SQS_QUEUE_URL", "")
S3_ENDPOINT = os.environ.get("DSIMAGING_S3_ENDPOINT") or os.environ.get(
    "MINIO_ENDPOINT", "" if SQS_QUEUE_URL else "http://minio:9000"
)
S3_ACCESS_KEY = (
    os.environ.get("DSIMAGING_ACCESS_KEY")
    or os.environ.get("MINIO_ROOT_USER")
    or ""
)
S3_SECRET_KEY = (
    os.environ.get("DSIMAGING_SECRET_KEY")
    or os.environ.get("MINIO_ROOT_PASSWORD")
    or ""
)
AWS_REGION = os.environ.get("DSIMAGING_AWS_REGION") or os.environ.get(
    "AWS_REGION", "us-east-1"
)
BUCKET = os.environ.get("BUCKET_NAME", "imaging-data")
RECONCILE_INTERVAL_SECONDS = int(os.environ.get("RECONCILE_INTERVAL_SECONDS", "10"))
OPERATOR_TOKEN = os.environ.get("DSIMAGING_CONTROLLER_TOKEN", "").strip()
MAX_WEBHOOK_BODY_BYTES = int(os.environ.get(
    "DSIMAGING_MAX_WEBHOOK_BODY_BYTES", str(1024 * 1024)))
PUBLISH_LOCK = ".publish-lock"
MANAGED_ARTIFACTS = (
    "manifest.yaml",
    "indexes/content_hash_index.parquet",
    "indexes/masks_content_hash_index.parquet",
    "metadata/sample_manifests.parquet",
    "metadata/samples.parquet",
)
DATASET_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")

state_lock = threading.Lock()
dirty_datasets = set()
last_reconcile = {}
last_errors = {}


class PublishInProgress(Exception):
    """Raised when a dataset has an active publish lock."""


class RecoveryIncomplete(Exception):
    """Raised when previous managed artifacts could not be restored."""


class LockOwnershipLost(Exception):
    """Raised when this reconciliation no longer owns its dataset lock."""


def get_s3():
    import boto3
    kwargs = {"region_name": AWS_REGION}
    if S3_ENDPOINT:
        kwargs["endpoint_url"] = S3_ENDPOINT
    if S3_ACCESS_KEY and S3_SECRET_KEY:
        kwargs["aws_access_key_id"] = S3_ACCESS_KEY
        kwargs["aws_secret_access_key"] = S3_SECRET_KEY
    return boto3.client("s3", **kwargs)


def get_sqs():
    import boto3
    return boto3.client("sqs", region_name=AWS_REGION)


def extract_dataset_id_from_source_key(key):
    """Extract dataset_id from datasets/<id>/source/{images,masks}/... keys."""
    parts = key.split("/")
    if (
        len(parts) >= 5
        and parts[0] == "datasets"
        and parts[2] == "source"
        and parts[3] in {"images", "masks"}
        and DATASET_ID_RE.fullmatch(parts[1])
    ):
        return parts[1]
    return None


def mark_dirty(dataset_id):
    with state_lock:
        dirty_datasets.add(dataset_id)


def iter_s3_event_records(events):
    """Yield normalized records from S3/MinIO event JSON."""
    if isinstance(events, (bytes, bytearray)):
        events = json.loads(events.decode())
    elif isinstance(events, str):
        events = json.loads(events)
    for record in events.get("Records", []):
        raw_key = record.get("s3", {}).get("object", {}).get("key", "")
        if not raw_key:
            continue
        yield {
            "key": unquote_plus(raw_key),
            "event_name": record.get("eventName", ""),
        }


def handle_s3_event_payload(events):
    """Mark datasets dirty for source image/mask S3 events."""
    count = 0
    for item in iter_s3_event_records(events):
        dataset_id = extract_dataset_id_from_source_key(item["key"])
        if dataset_id:
            log.info(
                "Source event received for dataset %s (%s)",
                dataset_id, item["event_name"],
            )
            mark_dirty(dataset_id)
            count += 1
    return count


def pop_dirty_batch():
    with state_lock:
        batch = sorted(dirty_datasets)
        dirty_datasets.clear()
    return batch


def record_success(dataset_id, n_samples, n_masks=0):
    with state_lock:
        last_reconcile[dataset_id] = {
            "at": utc_now(),
        }
        last_errors.pop(dataset_id, None)


def record_failure(dataset_id, error):
    with state_lock:
        dirty_datasets.add(dataset_id)
        last_errors[dataset_id] = {
            "at": utc_now(),
        }


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path.startswith("/reconcile/"):
            if not self.require_operator():
                return
            dataset_id = self.path.split("/reconcile/", 1)[1]
            if not DATASET_ID_RE.fullmatch(dataset_id or ""):
                self.write_error(400, "invalid request")
                return
            try:
                n_samples, n_masks = reconcile_dataset(dataset_id)
                record_success(dataset_id, n_samples, n_masks)
                self.write_json({"status": "ok"})
            except PublishInProgress:
                mark_dirty(dataset_id)
                self.write_error(409, "busy")
            except Exception as e:
                record_failure(dataset_id, e)
                log.error("Manual reconciliation failed for dataset %s", dataset_id)
                self.write_error(500, "reconciliation failed")
            return

        if self.path != "/webhook/minio":
            self.write_error(404, "not found")
            return

        try:
            content_length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            self.write_error(400, "invalid request")
            return
        if content_length < 0 or content_length > MAX_WEBHOOK_BODY_BYTES:
            self.close_connection = True
            self.write_error(413, "request too large")
            return
        body = self.rfile.read(content_length)
        try:
            handle_s3_event_payload(body)
        except Exception:
            log.error("Webhook event could not be parsed")
            self.write_error(400, "invalid request")
            return

        self.write_json({"status": "ok"})

    def do_GET(self):
        if self.path in {"/health", "/healthz"}:
            self.write_json({"status": "ok"})
            return

        if self.path == "/datasets":
            if not self.require_operator():
                return
            try:
                self.write_json({"datasets": list_datasets()})
            except Exception:
                log.error("Dataset inventory could not be listed")
                self.write_error(500, "inventory unavailable")
            return

        self.write_error(404, "not found")

    def write_json(self, payload):
        body = json.dumps(payload, sort_keys=True).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def write_error(self, status, message):
        body = json.dumps({"error": message}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def require_operator(self):
        if not OPERATOR_TOKEN:
            self.write_error(404, "not found")
            return False
        value = self.headers.get("Authorization", "")
        expected = f"Bearer {OPERATOR_TOKEN}"
        if not hmac.compare_digest(value, expected):
            self.write_error(403, "forbidden")
            return False
        return True

    def log_message(self, fmt, *args):
        pass


def list_datasets():
    s3 = get_s3()
    datasets = []
    paginator = s3.get_paginator("list_objects_v2")
    with state_lock:
        dirty = set(dirty_datasets)
        reconcile_snapshot = dict(last_reconcile)
        error_snapshot = dict(last_errors)

    for page in paginator.paginate(Bucket=BUCKET, Prefix="datasets/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            dataset_id = cp["Prefix"].strip("/").split("/")[-1]
            if not DATASET_ID_RE.fullmatch(dataset_id):
                continue
            if not prefix_has_current_objects(s3, f"datasets/{dataset_id}/"):
                continue
            has_manifest = object_exists(s3, f"datasets/{dataset_id}/manifest.yaml")
            datasets.append({
                "dataset_id": dataset_id,
                "status": "published" if has_manifest else "incomplete",
                "dirty": dataset_id in dirty,
                "last_reconcile_at": (
                    reconcile_snapshot.get(dataset_id) or {}).get("at"),
                "has_error": dataset_id in error_snapshot,
            })
    return datasets


def object_exists(s3, key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def prefix_has_current_objects(s3, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if object_exists(s3, obj["Key"]):
                return True
    return False


def reconcile_loop():
    while True:
        batch = pop_dirty_batch()
        for dataset_id in batch:
            try:
                n_samples, n_masks = reconcile_dataset(dataset_id)
                record_success(dataset_id, n_samples, n_masks)
                log.info("Reconciled dataset %s", dataset_id)
            except PublishInProgress:
                mark_dirty(dataset_id)
                log.info("Reconcile deferred for dataset %s: publish in progress", dataset_id)
            except Exception as e:
                record_failure(dataset_id, e)
                log.error("Reconcile failed for dataset %s", dataset_id)
        time.sleep(RECONCILE_INTERVAL_SECONDS)


def sqs_loop():
    if not SQS_QUEUE_URL:
        return
    sqs = get_sqs()
    log.info("SQS worker enabled: %s", SQS_QUEUE_URL)
    while True:
        try:
            process_sqs_messages(sqs, SQS_QUEUE_URL)
        except Exception:
            log.error("SQS worker failed")
            time.sleep(5)


def process_sqs_messages(sqs, queue_url, wait_time_seconds=20):
    """Process one long-poll batch from SQS."""
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=wait_time_seconds,
    )
    processed = 0
    for message in response.get("Messages", []):
        body = json.loads(message.get("Body", "{}"))
        if "Message" in body:
            body = json.loads(body["Message"])
        handle_s3_event_payload(body)
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message["ReceiptHandle"],
        )
        processed += 1
    return processed


def reconcile_dataset(dataset_id):
    if not isinstance(dataset_id, str) or not DATASET_ID_RE.fullmatch(dataset_id):
        raise ValueError("invalid dataset id")
    s3 = get_s3()
    prefix = f"datasets/{dataset_id}"
    publish_lock = acquire_publish_lock(s3, prefix)
    release_lock = True
    completed = False
    try:
        objects = list_objects(s3, f"{prefix}/source/images/")
        mask_objects = list_objects(s3, f"{prefix}/source/masks/")
        samples = scan_s3_images(s3, prefix, objects)
        masks = scan_s3_masks(
            s3, prefix, mask_objects,
            sample_ids=[sample["sample_id"] for sample in samples],
        )
        if not samples:
            manifest = read_existing_manifest(s3, prefix)
            metadata_contract_from_manifest(manifest)
            read_existing_samples_metadata(s3, prefix)
            assert_source_inventory_unchanged(
                s3, prefix, objects, mask_objects)
            assert_publish_lock_owned(s3, publish_lock)
            deleted = delete_dataset_artifacts(s3, prefix)
            if deleted:
                log.info(
                    "Removed %s managed artifact(s) for dataset %s because no source images remain",
                    deleted,
                    dataset_id,
                )
            completed = True
            return 0, len(masks)
        write_dataset_artifacts(
            s3, prefix, dataset_id, samples, masks,
            expected_images=objects, expected_masks=mask_objects,
            publish_lock=publish_lock,
        )
        completed = True
        return len(samples), len(masks)
    except RecoveryIncomplete:
        release_lock = False
        raise
    finally:
        if release_lock:
            released = release_publish_lock(s3, publish_lock)
            if completed and not released:
                raise LockOwnershipLost(
                    "dataset reconciliation lock ownership was lost")


def acquire_publish_lock(s3, prefix):
    owner = uuid.uuid4().hex
    key = f"{prefix}/{PUBLISH_LOCK}"
    try:
        response = s3.put_object(
            Bucket=BUCKET, Key=key,
            Body=json.dumps({
                "status": "reconciling", "owner": owner,
            }).encode("utf-8"),
            ContentType="application/json", IfNoneMatch="*",
        )
    except Exception:
        if object_exists(s3, key):
            raise PublishInProgress()
        raise
    return {"key": key, "owner": owner, "etag": response.get("ETag")}


def release_publish_lock(s3, publish_lock):
    if not publish_lock_is_owned(s3, publish_lock):
        return False
    key = publish_lock["key"]
    kwargs = {"Bucket": BUCKET, "Key": key}
    if publish_lock.get("etag"):
        kwargs["IfMatch"] = publish_lock["etag"]
    try:
        s3.delete_object(**kwargs)
    except Exception:
        return False
    return True


def publish_lock_is_owned(s3, publish_lock):
    key = publish_lock["key"]
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
        body = response["Body"]
        try:
            current = json.loads(body.read())
        finally:
            body.close()
    except Exception:
        return False
    return current.get("owner") == publish_lock["owner"]


def assert_publish_lock_owned(s3, publish_lock):
    if not publish_lock_is_owned(s3, publish_lock):
        raise LockOwnershipLost("dataset reconciliation lock ownership was lost")


def delete_dataset_artifacts(s3, prefix):
    previous = snapshot_dataset_artifacts(s3, prefix)
    keys = [
        f"{prefix}/{suffix}"
        for suffix in MANAGED_ARTIFACTS
        if object_exists(s3, f"{prefix}/{suffix}")
    ]
    try:
        deleted = delete_current_keys(s3, keys)
        if deleted != len(keys):
            raise RuntimeError("managed artifact deletion was incomplete")
    except Exception:
        try:
            restore_dataset_artifacts(s3, prefix, previous)
        except Exception as recovery_error:
            raise RecoveryIncomplete(
                "previous managed artifacts could not be restored"
            ) from recovery_error
        raise
    return deleted


def delete_current_keys(s3, keys):
    deleted = 0
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        if not chunk:
            continue
        response = s3.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )
        deleted += len(chunk) - len(response.get("Errors", []))
    return deleted


def list_objects(s3, prefix):
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append({
                "key": obj["Key"],
                "size": int(obj["Size"]),
                "last_modified": obj["LastModified"].isoformat(),
                "etag": obj.get("ETag", "").strip('"') or None,
            })
    return objects


def scan_s3_images(s3, prefix, objects):
    return core_scan_s3_images(s3, BUCKET, prefix, objects)


def scan_s3_masks(s3, prefix, objects, sample_ids=None):
    return core_scan_s3_masks(s3, BUCKET, prefix, objects, sample_ids=sample_ids)


def write_dataset_artifacts(s3, prefix, dataset_id, samples, masks=None, *,
                            expected_images=None, expected_masks=None,
                            publish_lock=None):
    existing_manifest = read_existing_manifest(s3, prefix)
    contract = metadata_contract_from_manifest(existing_manifest)
    extra_metadata = read_existing_samples_metadata(s3, prefix)
    masks = masks or []
    with tempfile.TemporaryDirectory() as tmpdir:
        uploads = [
            ("indexes/content_hash_index.parquet",
             write_parquet(tmpdir, "content_hash_index.parquet",
                           build_hash_index(prefix, samples, source_path="images"))),
            ("metadata/sample_manifests.parquet",
             write_parquet(tmpdir, "sample_manifests.parquet",
                           build_sample_manifests(samples))),
            ("metadata/samples.parquet",
             write_parquet(tmpdir, "samples.parquet",
                           build_samples_metadata(
                               samples, extra_metadata,
                               privacy_unit_col=contract["privacy_unit_col"],
                               label_col=contract.get("label_col"),
                           ))),
            ("manifest.yaml",
             write_yaml(tmpdir, "manifest.yaml",
                        generate_manifest(
                            dataset_id, prefix,
                            existing_manifest.get("modality") or "unknown",
                            has_masks=bool(masks),
                            privacy_unit_col=contract["privacy_unit_col"],
                            label_col=contract.get("label_col"),
                            existing_manifest=existing_manifest,
                        ))),
        ]
        if masks:
            uploads.append(
                ("indexes/masks_content_hash_index.parquet",
                 write_parquet(tmpdir, "masks_content_hash_index.parquet",
                               build_hash_index(prefix, masks, source_path="masks")))
            )
        # Generate and validate everything before changing the published set.
        # Upload the manifest last and restore the prior derived objects if an
        # upload fails midway.
        previous = snapshot_dataset_artifacts(s3, prefix)
        manifest_upload = uploads.pop(3)
        try:
            if publish_lock is not None:
                assert_publish_lock_owned(s3, publish_lock)
            for rel_key, path in uploads:
                s3.upload_file(path, BUCKET, f"{prefix}/{rel_key}")
            if not masks:
                mask_index_key = f"{prefix}/indexes/masks_content_hash_index.parquet"
                if object_exists(s3, mask_index_key):
                    if delete_current_keys(s3, [mask_index_key]) != 1:
                        raise RuntimeError("stale mask index could not be removed")
            if expected_images is not None and expected_masks is not None:
                assert_source_inventory_unchanged(
                    s3, prefix, expected_images, expected_masks)
            if publish_lock is not None:
                assert_publish_lock_owned(s3, publish_lock)
            rel_key, path = manifest_upload
            s3.upload_file(path, BUCKET, f"{prefix}/{rel_key}")
        except LockOwnershipLost:
            raise
        except Exception:
            try:
                restore_dataset_artifacts(s3, prefix, previous)
            except Exception as recovery_error:
                raise RecoveryIncomplete(
                    "previous managed artifacts could not be restored"
                ) from recovery_error
            raise


def build_hash_index(prefix, samples, source_path="images"):
    return core_build_hash_index(samples, BUCKET, prefix, source_path=source_path)


def source_inventory(objects):
    """Return the stable fields that define one current source roster."""
    return sorted(
        (obj["key"], int(obj.get("size", 0)), obj.get("etag"))
        for obj in objects
    )


def assert_source_inventory_unchanged(s3, prefix, images, masks):
    current_images = list_objects(s3, f"{prefix}/source/images/")
    current_masks = list_objects(s3, f"{prefix}/source/masks/")
    if (source_inventory(current_images) != source_inventory(images) or
            source_inventory(current_masks) != source_inventory(masks)):
        raise RuntimeError("dataset source roster changed during reconciliation")


def build_sample_manifests(samples):
    return core_build_sample_manifests(samples)


def build_samples_metadata(samples, extra_metadata=None, *,
                           privacy_unit_col, label_col=None):
    return core_build_samples_metadata(
        samples, extra_metadata=extra_metadata,
        privacy_unit_col=privacy_unit_col, label_col=label_col,
    )


def read_existing_samples_metadata(s3, prefix):
    key = f"{prefix}/metadata/samples.parquet"
    if not object_exists(s3, key):
        raise ValueError("samples metadata is missing")
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
        body = response["Body"]
        try:
            data = body.read()
        finally:
            body.close()
    except Exception as exc:
        raise ValueError("samples metadata could not be read") from exc
    if not data:
        raise ValueError("samples metadata is empty")
    try:
        return pq.read_table(pa.BufferReader(data))
    except Exception as exc:
        raise ValueError("samples metadata is corrupt") from exc


def generate_manifest(dataset_id, prefix, modality, has_masks=False, *,
                      privacy_unit_col, label_col=None,
                      existing_manifest=None):
    return core_generate_manifest(
        dataset_id, BUCKET, prefix, modality=modality, has_masks=has_masks,
        privacy_unit_col=privacy_unit_col, label_col=label_col,
        existing_manifest=existing_manifest,
    )


def read_existing_manifest(s3, prefix):
    key = f"{prefix}/manifest.yaml"
    if not object_exists(s3, key):
        raise ValueError("dataset manifest is missing; publish with dsimaging-admin first")
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
        body = response["Body"]
        try:
            manifest = yaml.safe_load(body.read())
        finally:
            body.close()
    except Exception as exc:
        raise ValueError("dataset manifest is corrupt") from exc
    if not isinstance(manifest, dict):
        raise ValueError("dataset manifest must be a mapping")
    validate_manifest_scope(manifest, BUCKET, prefix)
    return manifest


def snapshot_dataset_artifacts(s3, prefix):
    snapshot = {}
    for suffix in MANAGED_ARTIFACTS:
        key = f"{prefix}/{suffix}"
        if not object_exists(s3, key):
            continue
        response = s3.get_object(Bucket=BUCKET, Key=key)
        body = response["Body"]
        try:
            snapshot[suffix] = body.read()
        finally:
            body.close()
    return snapshot


def restore_dataset_artifacts(s3, prefix, snapshot):
    for suffix in MANAGED_ARTIFACTS:
        key = f"{prefix}/{suffix}"
        if suffix not in snapshot and object_exists(s3, key):
            if delete_current_keys(s3, [key]) != 1:
                raise RuntimeError("new managed artifact could not be removed")
    for suffix in MANAGED_ARTIFACTS:
        if suffix == "manifest.yaml" or suffix not in snapshot:
            continue
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}/{suffix}", Body=snapshot[suffix])
    if "manifest.yaml" in snapshot:
        s3.put_object(
            Bucket=BUCKET, Key=f"{prefix}/manifest.yaml",
            Body=snapshot["manifest.yaml"], ContentType="application/yaml",
        )


def existing_modality(s3, prefix, fallback):
    try:
        response = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/manifest.yaml")
        body = response["Body"]
        try:
            manifest = yaml.safe_load(body.read()) or {}
        finally:
            body.close()
        return manifest.get("modality") or fallback
    except Exception:
        return fallback


def write_parquet(tmpdir, filename, table):
    path = os.path.join(tmpdir, filename)
    pq.write_table(table, path)
    return path


def write_yaml(tmpdir, filename, payload):
    path = os.path.join(tmpdir, filename)
    with open(path, "w") as f:
        yaml.dump(payload, f, default_flow_style=False, sort_keys=False)
    return path


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def main():
    port = int(os.environ.get("PORT", "8080"))
    thread = threading.Thread(target=reconcile_loop, daemon=True)
    thread.start()
    if SQS_QUEUE_URL:
        sqs_thread = threading.Thread(target=sqs_loop, daemon=True)
        sqs_thread.start()
    server = HTTPServer(("0.0.0.0", port), Handler)
    log.info("Controller listening on port %s", port)
    log.info("S3 endpoint: %s, Bucket: %s", S3_ENDPOINT or "<aws-default>", BUCKET)
    log.info("AWS region: %s", AWS_REGION)
    log.info("Reconcile interval: %ss", RECONCILE_INTERVAL_SECONDS)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")
        server.server_close()


if __name__ == "__main__":
    main()
