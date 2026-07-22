# src/qc/report/publisher.py
import logging
import os

import boto3

logger = logging.getLogger(__name__)

_s3 = None


def _client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def publish_artifacts(
    output_path: str,
    client: str,
    domain: str,
    run_date: str,
    publish_cfg: dict,
) -> None:
    """Upload EBS artifacts for one client/domain/date to the analytics S3 bucket.

    Only issues.parquet is uploaded — delta_report.json and summary.json are
    EBS-only operational artifacts. Parquet keeps Hive partitioning so Athena
    discovers it automatically.

    Skips silently if publish.enabled is False.
    """
    if not publish_cfg.get("enabled"):
        return

    bucket = publish_cfg["bucket"]
    prefix = publish_cfg.get("prefix", "qc-output").rstrip("/")

    local_parquet = os.path.join(
        output_path, "qc_issues",
        f"client={client}", f"domain={domain}", f"date={run_date}",
        "issues.parquet",
    )
    if not os.path.exists(local_parquet):
        logger.warning("publish: parquet not found, skipping upload: %s", local_parquet)
        return

    s3_key = f"{prefix}/qc_issues/client={client}/domain={domain}/date={run_date}/issues.parquet"

    try:
        _client().upload_file(local_parquet, bucket, s3_key)
        logger.info(
            "published client=%s domain=%s date=%s -> s3://%s/%s",
            client, domain, run_date, bucket, s3_key,
        )
    except Exception as exc:
        logger.error(
            "publish failed client=%s domain=%s error_type=%s",
            client, domain, type(exc).__name__,
        )
