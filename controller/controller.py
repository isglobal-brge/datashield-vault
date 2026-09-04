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
STARTUP_RECOVERY_MAX_BACKOFF_SECONDS = 30
LOCK_OBSERVE_RETRIES = 3
LOCK_OBSERVE_DELAY_SECONDS = 0.05
OPERATOR_TOKEN = os.environ.get("DSIMAGING_CONTROLLER_TOKEN", "").strip()
WEBHOOK_TOKEN = os.environ.get("DSIMAGING_WEBHOOK_TOKEN", "").strip()
MAX_WEBHOOK_BODY_BYTES = int(os.environ.get(
    "DSIMAGING_MAX_WEBHOOK_BODY_BYTES", str(1024 * 1024)))
PUBLISH_LOCK = ".publish-lock"
DIRTY_PREFIX = "_controller/dirty/"
DIRTY_MIGRATION_KEY = "_controller/migrations/durable-dirty-v1.complete"
DIRTY_MIGRATION_MARKER_ID = "0" * 32
MANAGED_ARTIFACTS = (
    "manifest.yaml",
    "indexes/content_hash_index.parquet",
    "indexes/masks_content_hash_index.parquet",
    "metadata/sample_manifests.parquet",
    "metadata/samples.parquet",
)
DATASET_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
DIRTY_MARKER_ID_RE = re.compile(r"^[0-9a-f]{32}$")

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


class InvalidDatasetContent(ValueError):
    """Raised when published dataset content fails deterministic validation."""


class InvalidSourceContent(InvalidDatasetContent):
    """Raised when source images or masks fail deterministic validation."""


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


def dirty_marker_key(dataset_id, marker_id):
    return f"{DIRTY_PREFIX}{dataset_id}/{marker_id}"


def require_bucket_versioning(s3):
    try:
        status = s3.get_bucket_versioning(Bucket=BUCKET).get("Status")
    except Exception as exc:
        raise RuntimeError("bucket versioning could not be verified") from exc
    if status != "Enabled":
        raise RuntimeError("bucket versioning must be enabled")


def usable_version_id(value):
    return isinstance(value, str) and value not in ("", "null")


def require_object_version(s3, key):
    version_id = s3.head_object(Bucket=BUCKET, Key=key).get("VersionId")
    if not usable_version_id(version_id):
        raise RuntimeError("object has no usable version ID")
    return version_id


def put_dirty_marker(s3, dataset_id, marker_id, *, mark_memory=True):
    key = dirty_marker_key(dataset_id, marker_id)
    response = s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps({"event_id": marker_id}).encode("utf-8"),
        ContentType="application/json",
        IfNoneMatch="*",
    )
    version_id = response.get("VersionId")
    if not usable_version_id(version_id):
        raise RuntimeError("versioned dirty marker could not be established")
    if mark_memory:
        mark_dirty(dataset_id)
    return {
        "key": key,
        "etag": response.get("ETag"),
        "version_id": version_id,
    }


def persist_dirty_marker(s3, dataset_id, *, mark_memory=True):
    """Persist work before an event delivery can be acknowledged."""
    require_bucket_versioning(s3)
    return put_dirty_marker(
        s3, dataset_id, uuid.uuid4().hex, mark_memory=mark_memory)


def iter_s3_event_records(events):
    """Yield normalized records from S3/MinIO event JSON."""
    if isinstance(events, (bytes, bytearray)):
        events = json.loads(events.decode())
    elif isinstance(events, str):
        events = json.loads(events)
    if not isinstance(events, dict):
        raise ValueError("event envelope must be a mapping")
    records = events.get("Records", [])
    if not isinstance(records, list):
        raise ValueError("event Records must be a list")
    for record in records:
        if not isinstance(record, dict):
            continue
        s3_record = record.get("s3")
        if not isinstance(s3_record, dict):
            continue
        object_record = s3_record.get("object")
        if not isinstance(object_record, dict):
            continue
        raw_key = object_record.get("key")
        if not isinstance(raw_key, str) or not raw_key:
            continue
        yield {
            "key": unquote_plus(raw_key),
            "event_name": record.get("eventName", ""),
        }


def dataset_ids_from_s3_event_payload(events):
    """Return canonical dataset IDs affected by an S3 event payload."""
    dataset_ids = []
    seen = set()
    for item in iter_s3_event_records(events):
        dataset_id = extract_dataset_id_from_source_key(item["key"])
        if dataset_id and dataset_id not in seen:
            log.info(
                "Source event received for dataset %s (%s)",
                dataset_id, item["event_name"],
            )
            seen.add(dataset_id)
            dataset_ids.append(dataset_id)
    return dataset_ids


def persist_dirty_datasets(dataset_ids, *, mark_memory=True):
    if not dataset_ids:
        return 0
    s3 = get_s3()
    require_bucket_versioning(s3)
    for dataset_id in dataset_ids:
        put_dirty_marker(
            s3, dataset_id, uuid.uuid4().hex, mark_memory=mark_memory)
    return len(dataset_ids)


def handle_s3_event_payload(events):
    """Durably queue datasets affected by source image/mask S3 events."""
    return persist_dirty_datasets(dataset_ids_from_s3_event_payload(events))


def pop_dirty_batch():
    with state_lock:
        batch = sorted(dirty_datasets)
        dirty_datasets.clear()
    return batch


def migrate_legacy_pending_datasets():
    """Seed durable work once when upgrading from the in-memory queue."""
    s3 = get_s3()
    require_bucket_versioning(s3)
    if object_exists(s3, DIRTY_MIGRATION_KEY):
        require_object_version(s3, DIRTY_MIGRATION_KEY)
        return 0

    dataset_ids = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
            Bucket=BUCKET, Prefix="datasets/", Delimiter="/"):
        for item in page.get("CommonPrefixes", []):
            prefix = item.get("Prefix")
            if not isinstance(prefix, str):
                continue
            parts = prefix.split("/")
            if (len(parts) == 3 and parts[0] == "datasets" and not parts[2]
                    and DATASET_ID_RE.fullmatch(parts[1])):
                dataset_ids.add(parts[1])

    for dataset_id in sorted(dataset_ids):
        key = dirty_marker_key(dataset_id, DIRTY_MIGRATION_MARKER_ID)
        if object_exists(s3, key):
            require_object_version(s3, key)
            continue
        try:
            put_dirty_marker(
                s3, dataset_id, DIRTY_MIGRATION_MARKER_ID,
                mark_memory=False,
            )
        except Exception:
            if not object_exists(s3, key):
                raise
            require_object_version(s3, key)

    try:
        response = s3.put_object(
            Bucket=BUCKET,
            Key=DIRTY_MIGRATION_KEY,
            Body=json.dumps({"status": "complete"}).encode("utf-8"),
            ContentType="application/json",
            IfNoneMatch="*",
        )
    except Exception:
        if not object_exists(s3, DIRTY_MIGRATION_KEY):
            raise
        require_object_version(s3, DIRTY_MIGRATION_KEY)
    else:
        if not usable_version_id(response.get("VersionId")):
            raise RuntimeError("versioned migration marker could not be established")
    return len(dataset_ids)


def enqueue_persisted_dirty_datasets():
    """Recover only event work durably acknowledged by this controller."""
    s3 = get_s3()
    require_bucket_versioning(s3)
    dataset_ids = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=DIRTY_PREFIX):
        for item in page.get("Contents", []):
            key = item.get("Key")
            dataset_id = dataset_id_from_dirty_marker_key(key)
            if dataset_id:
                dataset_ids.add(dataset_id)
    with state_lock:
        dirty_datasets.update(dataset_ids)
    return len(dataset_ids)


def dataset_id_from_dirty_marker_key(key):
    if not isinstance(key, str) or not key.startswith(DIRTY_PREFIX):
        return None
    parts = key[len(DIRTY_PREFIX):].split("/")
    if (len(parts) != 2 or not DATASET_ID_RE.fullmatch(parts[0]) or
            not DIRTY_MARKER_ID_RE.fullmatch(parts[1])):
        return None
    return parts[0]


def recover_then_reconcile():
    """Recover durable work with backoff, then enter the normal loop."""
    backoff = 1
    while True:
        try:
            migrated = migrate_legacy_pending_datasets()
            enqueue_persisted_dirty_datasets()
            if migrated:
                log.info("Durable reconciliation queue migration completed")
            log.info("Persisted reconciliation work queued")
            break
        except Exception:
            log.error("Persisted reconciliation work could not be listed")
            time.sleep(backoff)
            backoff = min(backoff * 2, STARTUP_RECOVERY_MAX_BACKOFF_SECONDS)
    reconcile_loop()


def record_success(dataset_id, n_samples, n_masks=0):
    with state_lock:
        last_reconcile[dataset_id] = {
            "at": utc_now(),
        }
        last_errors.pop(dataset_id, None)


def record_failure(dataset_id, error, retry=True):
    with state_lock:
        if retry:
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
                persist_dirty_datasets([dataset_id], mark_memory=False)
                n_samples, n_masks = reconcile_pending_dataset(dataset_id)
                record_success(dataset_id, n_samples, n_masks)
                self.write_json({"status": "ok"})
            except PublishInProgress:
                mark_dirty(dataset_id)
                self.write_error(409, "busy")
            except InvalidDatasetContent as e:
                record_failure(dataset_id, e, retry=False)
                log.error("Manual reconciliation failed for dataset %s", dataset_id)
                self.write_error(500, "reconciliation failed")
            except Exception as e:
                record_failure(dataset_id, e)
                log.error("Manual reconciliation failed for dataset %s", dataset_id)
                self.write_error(500, "reconciliation failed")
            return

        if self.path != "/webhook/minio":
            self.write_error(404, "not found")
            return
        if not self.require_webhook():
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
            dataset_ids = dataset_ids_from_s3_event_payload(body)
        except (AttributeError, KeyError, RecursionError, TypeError, UnicodeError,
                ValueError):
            log.error("Webhook event could not be parsed")
            self.write_error(400, "invalid request")
            return
        try:
            persist_dirty_datasets(dataset_ids)
        except Exception:
            log.error("Webhook event could not be persisted")
            self.write_error(503, "temporarily unavailable")
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
        if not bearer_matches(value, OPERATOR_TOKEN):
            self.write_error(403, "forbidden")
            return False
        return True

    def require_webhook(self):
        if not WEBHOOK_TOKEN:
            self.write_error(404, "not found")
            return False
        value = self.headers.get("Authorization", "")
        if not bearer_matches(value, WEBHOOK_TOKEN):
            self.write_error(403, "forbidden")
            return False
        return True

    def log_message(self, fmt, *args):
        pass


def bearer_matches(value, token):
    """Compare bearer credentials without rejecting non-ASCII input noisily."""
    if not isinstance(value, str) or not isinstance(token, str):
        return False
    try:
        return hmac.compare_digest(
            value.encode("utf-8"), f"Bearer {token}".encode("utf-8"))
    except UnicodeEncodeError:
        return False


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
    except Exception as exc:
        if object_not_found(exc):
            return False
        raise


def object_not_found(exc):
    if isinstance(exc, KeyError):
        return True
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    code = response.get("Error", {}).get("Code")
    return str(code) in {"404", "NoSuchKey", "NotFound"}


def dirty_marker_snapshot(s3, dataset_id):
    """Snapshot current marker objects; later events use different UUID keys."""
    markers = []
    prefix = f"{DIRTY_PREFIX}{dataset_id}/"
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item.get("Key")
            if dataset_id_from_dirty_marker_key(key) != dataset_id:
                continue
            try:
                response = s3.head_object(Bucket=BUCKET, Key=key)
            except Exception as exc:
                if object_not_found(exc):
                    continue
                raise
            version_id = response.get("VersionId")
            if not usable_version_id(version_id):
                raise RuntimeError("dirty marker has no usable version ID")
            markers.append({
                "key": key,
                "version_id": version_id,
            })
    return markers


def clear_dirty_markers(s3, markers):
    cleared = True
    for marker in markers:
        if not usable_version_id(marker.get("version_id")):
            cleared = False
            continue
        kwargs = {
            "Bucket": BUCKET,
            "Key": marker["key"],
            "VersionId": marker["version_id"],
        }
        try:
            s3.delete_object(**kwargs)
        except Exception as exc:
            if not object_not_found(exc):
                cleared = False
    return cleared


def reconcile_pending_dataset(dataset_id):
    """Reconcile one queued dataset and consume only its observed marker."""
    s3 = get_s3()
    require_bucket_versioning(s3)
    markers = dirty_marker_snapshot(s3, dataset_id)
    try:
        result = reconcile_dataset(dataset_id)
    except InvalidDatasetContent:
        if not clear_dirty_markers(s3, markers):
            mark_dirty(dataset_id)
        raise
    if not clear_dirty_markers(s3, markers):
        mark_dirty(dataset_id)
    return result


def prefix_has_current_objects(s3, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if object_exists(s3, obj["Key"]):
                return True
    return False


def reconcile_loop():
    while True:
        try:
            enqueue_persisted_dirty_datasets()
        except Exception:
            log.error("Persisted reconciliation work could not be refreshed")
        batch = pop_dirty_batch()
        for dataset_id in batch:
            try:
                n_samples, n_masks = reconcile_pending_dataset(dataset_id)
                record_success(dataset_id, n_samples, n_masks)
                log.info("Reconciled dataset %s", dataset_id)
            except PublishInProgress:
                mark_dirty(dataset_id)
                log.info("Reconcile deferred for dataset %s: publish in progress", dataset_id)
            except InvalidDatasetContent as e:
                record_failure(dataset_id, e, retry=False)
                log.error("Reconcile failed for dataset %s", dataset_id)
            except Exception as e:
                record_failure(dataset_id, e)
                log.error("Reconcile failed for dataset %s", dataset_id)
        time.sleep(RECONCILE_INTERVAL_SECONDS)


def sqs_loop():
    if not SQS_QUEUE_URL:
        return
    log.info("SQS worker enabled: %s", SQS_QUEUE_URL)
    sqs = None
    backoff = 1
    while True:
        try:
            if sqs is None:
                sqs = get_sqs()
            process_sqs_messages(sqs, SQS_QUEUE_URL)
            backoff = 1
        except Exception:
            sqs = None
            log.error("SQS worker failed")
            time.sleep(backoff)
            backoff = min(backoff * 2, STARTUP_RECOVERY_MAX_BACKOFF_SECONDS)


def process_sqs_messages(sqs, queue_url, wait_time_seconds=20):
    """Process one long-poll batch from SQS."""
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=wait_time_seconds,
    )
    processed = 0
    for message in response.get("Messages", []):
        try:
            body = json.loads(message.get("Body", "{}"))
            if "Message" in body:
                body = json.loads(body["Message"])
            dataset_ids = dataset_ids_from_s3_event_payload(body)
        except (AttributeError, KeyError, RecursionError, TypeError, ValueError,
                UnicodeError):
            # A malformed notification is not transient. Delete it so one
            # poison message cannot wedge the event consumer indefinitely.
            log.warning("Discarding malformed SQS notification")
        else:
            # Persistence is deliberately outside the malformed-message catch:
            # an S3 failure is transient and the notification must not be ACKed.
            persist_dirty_datasets(dataset_ids)
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
    try:
        objects = list_objects(
            s3, f"{prefix}/source/images/", include_version_ids=True)
        mask_objects = list_objects(
            s3, f"{prefix}/source/masks/", include_version_ids=True)
        try:
            samples = scan_s3_images(s3, prefix, objects)
            masks = scan_s3_masks(
                s3, prefix, mask_objects,
                sample_ids=[sample["sample_id"] for sample in samples],
            )
        except ValueError as exc:
            raise InvalidSourceContent(str(exc)) from exc
        if not samples:
            current_keys = {
                obj["key"] for obj in list_objects(s3, f"{prefix}/")
            }
            if current_keys == {publish_lock["key"]}:
                assert_source_inventory_unchanged(
                    s3, prefix, objects, mask_objects)
                assert_publish_lock_owned(s3, publish_lock)
                confirmed_keys = {
                    obj["key"] for obj in list_objects(s3, f"{prefix}/")
                }
                if confirmed_keys != {publish_lock["key"]}:
                    raise RuntimeError(
                        "dataset changed during deleted-prefix reconciliation"
                    )
                return 0, 0
            manifest = read_existing_manifest(s3, prefix)
            read_metadata_contract(manifest)
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
            return 0, len(masks)
        write_dataset_artifacts(
            s3, prefix, dataset_id, samples, masks,
            expected_images=objects, expected_masks=mask_objects,
            publish_lock=publish_lock,
        )
        return len(samples), len(masks)
    except RecoveryIncomplete:
        release_lock = False
        raise
    finally:
        if release_lock:
            released = release_publish_lock(s3, publish_lock)
            if not released:
                raise LockOwnershipLost(
                    "dataset reconciliation lock ownership was lost")


def acquire_publish_lock(s3, prefix):
    # Publish locks have no fenced lease shared with dsimaging-admin. Never
    # auto-delete an existing lock: an operator must verify and remove orphans.
    require_bucket_versioning(s3)
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
    except Exception as put_error:
        observation_error = None
        for attempt in range(LOCK_OBSERVE_RETRIES):
            try:
                current, identity = read_current_publish_lock(s3, key)
            except Exception as exc:
                observation_error = exc
                if attempt + 1 < LOCK_OBSERVE_RETRIES:
                    time.sleep(LOCK_OBSERVE_DELAY_SECONDS)
                continue
            if (
                isinstance(current, dict)
                and current.get("status") == "reconciling"
                and current.get("owner") == owner
            ):
                if not usable_version_id(identity.get("version_id")):
                    raise LockOwnershipLost(
                        "versioned reconciliation lock was not established")
                return {
                    "key": key,
                    "owner": owner,
                    "etag": identity.get("etag"),
                    "version_id": identity["version_id"],
                }
            raise PublishInProgress()
        raise put_error from observation_error
    version_id = response.get("VersionId")
    if not usable_version_id(version_id):
        try:
            current, identity = read_current_publish_lock(s3, key)
        except Exception as exc:
            raise LockOwnershipLost(
                "versioned reconciliation lock was not established") from exc
        if (
            not isinstance(current, dict)
            or current.get("status") != "reconciling"
            or current.get("owner") != owner
            or not usable_version_id(identity.get("version_id"))
        ):
            raise LockOwnershipLost(
                "versioned reconciliation lock was not established")
        version_id = identity["version_id"]
        response["ETag"] = identity.get("etag")
    return {
        "key": key,
        "owner": owner,
        "etag": response.get("ETag"),
        "version_id": version_id,
    }


def release_publish_lock(s3, publish_lock):
    if not publish_lock_is_owned(s3, publish_lock):
        return False
    if not usable_version_id(publish_lock.get("version_id")):
        return False
    key = publish_lock["key"]
    kwargs = {
        "Bucket": BUCKET,
        "Key": key,
        "VersionId": publish_lock["version_id"],
    }
    try:
        s3.delete_object(**kwargs)
    except Exception:
        return False
    return True


def publish_lock_is_owned(s3, publish_lock):
    key = publish_lock["key"]
    expected_etag = str(publish_lock.get("etag") or "").strip('"') or None
    expected_version = publish_lock.get("version_id")
    if not usable_version_id(expected_version):
        return False
    try:
        current, identity = read_current_publish_lock(s3, key)
    except Exception:
        return False
    return (
        (not expected_etag or identity["etag"] == expected_etag)
        and identity["version_id"] == expected_version
        and isinstance(current, dict)
        and current.get("status") == "reconciling"
        and current.get("owner") == publish_lock["owner"]
    )


def current_object_identity(s3, key):
    response = s3.head_object(Bucket=BUCKET, Key=key)
    return {
        "etag": str(response.get("ETag") or "").strip('"') or None,
        "version_id": response.get("VersionId"),
    }


def read_current_publish_lock(s3, key):
    before = current_object_identity(s3, key)
    response = s3.get_object(Bucket=BUCKET, Key=key)
    body = response["Body"]
    try:
        current = json.loads(body.read())
    finally:
        body.close()
    after = current_object_identity(s3, key)
    if before != after:
        raise RuntimeError("publication lock changed while it was read")
    return current, before


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


def list_objects(s3, prefix, *, include_version_ids=False):
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if include_version_ids:
                head = s3.head_object(Bucket=BUCKET, Key=obj["Key"])
                objects.append({
                    "key": obj["Key"],
                    "size": int(head.get("ContentLength", 0)),
                    "last_modified": head.get("LastModified").isoformat()
                    if head.get("LastModified") else None,
                    "etag": head.get("ETag", "").strip('"') or None,
                    "version_id": head.get("VersionId"),
                })
                continue
            objects.append({
                "key": obj["Key"],
                "size": int(obj["Size"]),
                "last_modified": obj["LastModified"].isoformat(),
                "etag": obj.get("ETag", "").strip('"') or None,
                "version_id": None,
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
    contract = read_metadata_contract(existing_manifest)
    extra_metadata = read_existing_samples_metadata(s3, prefix)
    masks = masks or []
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            uploads = [
                ("indexes/content_hash_index.parquet",
                 write_parquet(tmpdir, "content_hash_index.parquet",
                               build_hash_index(
                                   prefix, samples, source_path="images"))),
                ("metadata/sample_manifests.parquet",
                 write_parquet(tmpdir, "sample_manifests.parquet",
                               build_sample_manifests(samples))),
                ("metadata/samples.parquet",
                 write_parquet(tmpdir, "samples.parquet",
                               build_samples_metadata(
                                   samples, extra_metadata,
                                   privacy_unit_col=contract["privacy_unit_col"],
                                   label_col=contract.get("label_col"),
                                   label_levels=contract.get("label_levels"),
                               ))),
                ("manifest.yaml",
                 write_yaml(tmpdir, "manifest.yaml",
                            generate_manifest(
                                dataset_id, prefix,
                                existing_manifest.get("modality") or "unknown",
                                has_masks=bool(masks),
                                privacy_unit_col=contract["privacy_unit_col"],
                                label_col=contract.get("label_col"),
                                label_levels=contract.get("label_levels"),
                                existing_manifest=existing_manifest,
                            ))),
            ]
            if masks:
                uploads.append(
                    ("indexes/masks_content_hash_index.parquet",
                     write_parquet(
                         tmpdir, "masks_content_hash_index.parquet",
                         build_hash_index(prefix, masks, source_path="masks")))
                )
        except InvalidDatasetContent:
            raise
        except ValueError as exc:
            raise InvalidDatasetContent(str(exc)) from exc
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
        (
            obj["key"], int(obj.get("size", 0)), obj.get("etag"),
            obj.get("version_id"),
        )
        for obj in objects
    )


def assert_source_inventory_unchanged(s3, prefix, images, masks):
    current_images = list_objects(
        s3, f"{prefix}/source/images/", include_version_ids=True)
    current_masks = list_objects(
        s3, f"{prefix}/source/masks/", include_version_ids=True)
    if (source_inventory(current_images) != source_inventory(images) or
            source_inventory(current_masks) != source_inventory(masks)):
        raise RuntimeError("dataset source roster changed during reconciliation")


def build_sample_manifests(samples):
    return core_build_sample_manifests(samples)


def build_samples_metadata(samples, extra_metadata=None, *,
                           privacy_unit_col, label_col=None,
                           label_levels=None):
    return core_build_samples_metadata(
        samples, extra_metadata=extra_metadata,
        privacy_unit_col=privacy_unit_col, label_col=label_col,
        label_levels=label_levels,
    )


def read_existing_samples_metadata(s3, prefix):
    key = f"{prefix}/metadata/samples.parquet"
    if not object_exists(s3, key):
        raise InvalidDatasetContent("samples metadata is missing")
    response = s3.get_object(Bucket=BUCKET, Key=key)
    body = response["Body"]
    try:
        data = body.read()
    finally:
        body.close()
    if not data:
        raise InvalidDatasetContent("samples metadata is empty")
    try:
        return pq.read_table(pa.BufferReader(data))
    except (OSError, pa.ArrowInvalid, pa.ArrowTypeError) as exc:
        raise InvalidDatasetContent("samples metadata is corrupt") from exc


def generate_manifest(dataset_id, prefix, modality, has_masks=False, *,
                      privacy_unit_col, label_col=None, label_levels=None,
                      existing_manifest=None):
    return core_generate_manifest(
        dataset_id, BUCKET, prefix, modality=modality, has_masks=has_masks,
        privacy_unit_col=privacy_unit_col, label_col=label_col,
        label_levels=label_levels,
        existing_manifest=existing_manifest,
    )


def read_existing_manifest(s3, prefix):
    key = f"{prefix}/manifest.yaml"
    if not object_exists(s3, key):
        raise InvalidDatasetContent(
            "dataset manifest is missing; publish with dsimaging-admin first")
    response = s3.get_object(Bucket=BUCKET, Key=key)
    body = response["Body"]
    try:
        data = body.read()
    finally:
        body.close()
    try:
        manifest = yaml.safe_load(data)
    except (RecursionError, UnicodeError, yaml.YAMLError) as exc:
        raise InvalidDatasetContent("dataset manifest is corrupt") from exc
    if not isinstance(manifest, dict):
        raise InvalidDatasetContent("dataset manifest must be a mapping")
    try:
        validate_manifest_scope(manifest, BUCKET, prefix)
    except ValueError as exc:
        raise InvalidDatasetContent(str(exc)) from exc
    return manifest


def read_metadata_contract(manifest):
    try:
        return metadata_contract_from_manifest(manifest)
    except ValueError as exc:
        raise InvalidDatasetContent(str(exc)) from exc


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
    # HTTPServer binds and starts listening here, before durable recovery is
    # attempted. Incoming webhooks therefore wait or receive a non-2xx rather
    # than being lost while S3 is unavailable.
    server = HTTPServer(("0.0.0.0", port), Handler)
    if SQS_QUEUE_URL:
        sqs_thread = threading.Thread(target=sqs_loop, daemon=True)
        sqs_thread.start()
    thread = threading.Thread(target=recover_then_reconcile, daemon=True)
    thread.start()
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
