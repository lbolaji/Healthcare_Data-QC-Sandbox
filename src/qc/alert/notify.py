# src/qc/alert/notify.py
import logging

logger = logging.getLogger(__name__)


def notify_threshold_breach(client: str, domain: str, run_date: str, flags: list[dict]) -> None:
    error_count = sum(1 for f in flags if f.get("severity") == "error")
    warn_count = sum(1 for f in flags if f.get("severity") == "warn")
    logger.warning(
        "THRESHOLD_BREACH client=%s domain=%s run_date=%s errors=%d warnings=%d "
        "[SNS notify stubbed — configure SNS ARN to enable]",
        client, domain, run_date, error_count, warn_count,
    )
    # SNS publish intentionally disabled for sandbox:
    # sns = boto3.client("sns")
    # sns.publish(TopicArn=SNS_TOPIC_ARN, Message=...)
